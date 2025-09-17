use super::{
    ptr, unlikely, Allocator, Fallibility, Global, Group, InsertSlot, Layout, RawTableInner,
    TableLayout, TryReserveError,
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

/// 哈希表中key的关键能力描述
pub trait KeyDesc {
    /// 描述entry.key的hash函数
    fn hash(&self, ptr: *const u8) -> u64;
    /// 描述entry.key的equals函数
    fn equals(&self, a: *const u8, b: *const u8) -> bool;
}

/// 哈希表中单个entry的内存结构描述信息
pub struct Entry<K: KeyDesc> {
    /// K关键描述信息
    pub key_spec: K,
    /// K的内存大小
    pub key_size: u32,
    /// V的内存偏移量, K的偏移量为0
    pub val_offset: usize,
    /// (K, V)结构体的完整内存布局
    pub layout: Layout,
}

///
/// 面向原生内存的<K, V>哈希表
///
pub struct RawMap<K: KeyDesc, A: Allocator = Global> {
    entry: Entry<K>,
    /// caller可以指定自己的Allocator
    alloc: A,
    /// 内部swiss table
    inner: RawTableInner,
}

impl<K: KeyDesc, A: Allocator> RawMap<K, A> {
    ///
    /// 构造新的哈希表, 支持指定默认cap, 若为0则视为构造空的哈希表
    ///
    pub fn new(cap: usize, entry: Entry<K>, alloc: A) -> Result<Self, TryReserveError> {
        let table_layout = TableLayout::from(entry.layout);
        let inner = RawTableInner::fallible_with_capacity(
            &alloc,
            table_layout,
            cap,
            Fallibility::Fallible,
        )?;
        Ok(Self {
            entry,
            alloc,
            inner,
        })
    }

    ///
    /// 获取`key: &K`在此map中的`value: &V`, 出入参均为K/V的有效内存指针。
    /// 它的使用场景为`map.get(key)`, 计算过程中直接使用`self.hash_fn`和`self.eq_fn`
    ///
    pub fn access(&self, key: *const u8) -> Option<*const u8> {
        unsafe { self.find(key).map(|o| self.bucket(o) as *const u8) }
    }

    ///
    /// 获取`key: &K`在当前map中的“可赋值地址”, 出入参均为K/V的有效内存地址。
    ///
    /// 它的使用场景为`map.set(key, value)`, 只是过程分为两步走:
    /// 先计算key槽位物理地址, 然后向地址内写入value；此函数只负责第一步, 即按需扩容+返回value地址, 由caller写入数据
    ///
    pub fn assign(&mut self, key: *const u8) -> *mut u8 {
        let hash = self.entry.key_spec.hash(key);
        unsafe {
            self.check_growth(1).expect("map growth failure");

            let ptr = match self.find_or_insert(hash, key) {
                Ok(index) => self.bucket(index),
                Err(slot) => {
                    let old_ctrl = *self.inner.ctrl(slot.index);
                    self.inner.record_item_insert_at(slot.index, old_ctrl, hash);
                    let bucket = self.bucket(slot.index);
                    // 将key复制进去
                    ptr::copy_nonoverlapping(key, bucket, self.entry.key_size as usize);
                    bucket
                }
            };
            // 返回value内存地址
            ptr.offset(self.entry.val_offset as isize)
        }
    }

    ///
    /// 将other中的所有buckets导入当前map
    ///
    pub fn extend(&mut self, other: &Self) {
        unsafe {
            self.check_growth(other.len()).expect("map growth failure");

            for index in self.inner.full_buckets_indices() {
                let entry = self.bucket(index) as *const u8;
                let hash = self.entry.key_spec.hash(entry);
                let bucket = match self.find_or_insert(hash, entry) {
                    Ok(index) => self.bucket(index),
                    Err(slot) => {
                        let old_ctrl = *self.inner.ctrl(slot.index);
                        self.inner.record_item_insert_at(slot.index, old_ctrl, hash);
                        self.bucket(slot.index)
                    }
                };
                ptr::copy_nonoverlapping(entry, bucket, self.entry.layout.size())
            }
        }
    }

    ///
    /// 从当前map中删除指定key, 即将该key对应的Bucket软删除
    ///
    pub fn delete(&mut self, key: *const u8) {
        unsafe { self.find(key).map(|i| self.inner.erase(i)) };
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

    #[inline(always)]
    unsafe fn bucket(&self, index: usize) -> *mut u8 {
        self.inner.bucket_ptr(index, self.entry.layout.size())
    }

    #[inline(always)]
    unsafe fn find(&self, key: *const u8) -> Option<usize> {
        let hash = self.entry.key_spec.hash(key);
        let mut equals = |index| self.entry.key_spec.equals(key, self.bucket(index));
        self.inner.find_inner(hash, &mut equals)
    }

    #[inline(always)]
    unsafe fn find_or_insert(&self, hash: u64, key: *const u8) -> Result<usize, InsertSlot> {
        let mut equals = |index| self.entry.key_spec.equals(key, self.bucket(index));
        self.inner.find_or_find_insert_slot_inner(hash, &mut equals)
    }

    #[inline(always)]
    unsafe fn check_growth(&mut self, additional: usize) -> Result<(), TryReserveError> {
        if unlikely(additional > self.inner.growth_left) {
            self.do_growth(additional, Fallibility::Fallible)
        } else {
            Ok(())
        }
    }

    #[cold]
    #[inline(never)]
    unsafe fn do_growth(
        &mut self,
        additional: usize,
        fallibility: Fallibility,
    ) -> Result<(), TryReserveError> {
        self.inner.reserve_rehash_inner(
            &self.alloc,
            additional,
            &|table, index| {
                self.entry
                    .key_spec
                    .hash(table.bucket_ptr(index, self.entry.layout.size()))
            },
            fallibility,
            TableLayout::from(self.entry.layout),
            None,
        )
    }
}
