use super::{
    unlikely, Allocator, Fallibility, Global, Group, Layout, RawTableInner, TableLayout, Tag,
    TryReserveError,
};

impl From<Layout> for TableLayout {
    fn from(value: Layout) -> Self {
        Self {
            size: value.size(),
            ctrl_align: if value.align() > Group::WIDTH {
                value.align()
            } else {
                Group::WIDTH
            },
        }
    }
}

pub struct RawMap<A: Allocator = Global> {
    ///  Entry的内存布局
    layout: Layout,
    /// caller可以指定自己的Allocator
    alloc: A,
    /// 内部swiss table
    inner: RawTableInner,
}

impl<A: Allocator> RawMap<A> {
    ///
    /// 构造新的哈希表, 支持指定默认cap, 若为0则视为构造空的哈希表
    ///
    pub fn new(cap: usize, layout: Layout, alloc: A) -> Result<Self, TryReserveError> {
        let inner = unsafe {
            let table_layout = TableLayout::from(layout);
            RawTableInner::new_uninitialized(&alloc, table_layout, cap, Fallibility::Fallible)?
        };
        Ok(Self {
            layout,
            alloc,
            inner,
        })
    }

    ///
    /// 获取`key: &K`在此map中的`value: &V`, 出入参均为K/V的有效内存指针。
    /// 它的使用场景为`map.get(key)`, 计算过程中直接使用`self.hash_fn`和`self.eq_fn`
    ///
    pub unsafe fn access(&self, hash: u64, mut eq: impl FnMut(*mut u8) -> bool) -> Option<*mut u8> {
        let index = unsafe {
            self.inner
                .find_inner(hash, &mut |index| eq(self.bucket(index)))
        };
        index.map(|o| self.bucket(o))
    }

    ///
    /// 获取`key: &K`在当前map中的“可赋值地址”, 出入参均为K/V的有效内存地址。
    ///
    /// 它的使用场景为`map.set(key, value)`, 只是过程分为两步走:
    /// 先计算key槽位物理地址, 然后向地址内写入value；此函数只负责第一步, 即按需扩容+返回value地址, 由caller写入数据
    ///
    pub fn assign(
        &mut self,
        hash: u64,
        mut eq: impl FnMut(*const u8) -> bool,
        hasher: impl Fn(*const u8) -> u64,
    ) -> *mut u8 {
        unsafe { self.check_growth(1, hasher) };

        unsafe {
            match self
                .inner
                .find_or_find_insert_slot_inner(hash, &mut |index| eq(self.bucket(index)))
            {
                Ok(index) => self.bucket(index),
                Err(slot) => self.bucket(slot.index),
            }
        }
    }

    ///
    /// 从当前map中删除指定key, 即将该key对应的Bucket软删除
    ///
    pub fn delete(&mut self, key: *const u8) {
        todo!()
    }

    ///
    /// 清空当前map中的所有entries, 不需要卸载内存, 软删所有Bucket即可
    ///
    pub fn clear(&mut self) {
        self.inner.clear_no_drop();
    }

    ///
    /// 获取当前map中的entries数量
    ///
    pub fn len(&self) -> usize {
        self.inner.items
    }

    ///
    /// Swiss-Table可以视为一个稀疏的一维数组, 即部分slot为有效的Bucket,
    /// 它的迭代策略为[0...len]遍历所有槽位中的“FULL”状态并返回该槽位内存地址。
    ///
    /// 此方法是map迭代器的核心实现, 用于获取map底层一维数组中`[index, size)`的首个有效entry。
    ///
    pub fn next_entry(&self, index: usize) -> Option<(usize, *const u8)> {
        let mut curr_idx = index;
        loop {
            // 加载[curr_idx, curr_idx+16]的一组控制字节, 它可能是未对齐的
            let group = unsafe {
                let group_ptr = self.inner.ctrl(curr_idx);
                Group::load(group_ptr)
            };
            // 匹配FULL状态的位掩码
            let mask = group.match_full();
            if mask.any_bit_set() {
                let group_offset = mask.trailing_zeros();
                let matched_index = curr_idx + group_offset; // 此函数只关心next, 而非next_all
                let bucket = unsafe { self.bucket(matched_index) };
                return Some((matched_index, bucket));
            }
            curr_idx += Group::WIDTH;
            // 检查是否遍历到了末尾
            if curr_idx >= self.inner.buckets() {
                return None;
            }
        }
    }
}

// 一些私有的函数封装
impl<A: Allocator> RawMap<A> {
    /// 获取指定编号的entry地址, 此槽位必须是FULL状态的
    unsafe fn bucket(&self, index: usize) -> *mut u8 {
        self.inner.bucket_ptr(index, self.layout.size())
    }

    /// 检查当前哈希表是否需要扩容
    unsafe fn check_growth(
        &mut self,
        additional: usize,
        hasher: impl Fn(*const u8) -> u64,
    ) -> Result<(), TryReserveError> {
        if unlikely(additional > self.inner.growth_left) {
            self.do_growth(additional, hasher, Fallibility::Fallible)
        } else {
            Ok(())
        }
    }

    /// 执行扩容
    #[cold]
    #[inline(never)]
    unsafe fn do_growth(
        &mut self,
        additional: usize,
        hasher: impl Fn(*const u8) -> u64,
        fallibility: Fallibility,
    ) -> Result<(), TryReserveError> {
        self.inner.reserve_rehash_inner(
            &self.alloc,
            additional,
            &|table, index| hasher(table.bucket_ptr(index, self.layout.size())),
            fallibility,
            TableLayout::from(self.layout),
            None,
        )
    }
}
