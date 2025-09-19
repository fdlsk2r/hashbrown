use super::{
    unlikely, Allocator, Fallibility, Global, Group, Layout, PhantomData, RawTableInner,
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

impl From<EntryLayout> for TableLayout {
    fn from(value: EntryLayout) -> Self {
        Self {
            size: value.size as usize,
            ctrl_align: if value.align as usize > Group::WIDTH {
                value.align as usize
            } else {
                Group::WIDTH
            },
        }
    }
}

///
/// The memory layout of `map#Entry`, it should be `(K, V)` in most cases.
///
#[derive(Copy, Clone, Debug)]
pub struct EntryLayout {
    /// Indicates the total memory size of this entry.
    pub size: u32,
    /// Indicates where's the `V` in entry, [0...voff) is K, [voff, size) is V.
    pub voff: u16,
    /// Indicates the aligment of this entry, it should be usize in most cases.
    pub align: u16,
}

///
/// 哈希表内部的Entry规范, 用于caller注入自定义的实现细节
///
pub trait EntrySpec {
    /// Calculates specified key's hash value, it'll be used in bucket locating.
    unsafe fn hash(&self, k: *const u8) -> u64;
    /// Calculates whether k1 equals to k2 or not, it'll be used in `entry.key` comparing.
    unsafe fn equals(&self, k1: *const u8, k2: *const u8) -> bool;
    /// Assign other into k_ptr, equals to `self_entry.k = other`
    unsafe fn assign_key(&self, k_ptr: *const u8, other: *const u8);
    /// Assign other into v_ptr, equals to `self_entry.v = other`
    unsafe fn assign_value(&self, v_ptr: *const u8, other: *const u8);
}

///
/// 面向原生内存的<K, V>哈希表
///
pub struct RawTable2<E: EntrySpec, A: Allocator = Global> {
    layout: EntryLayout,
    spec: E,
    /// caller可以指定自己的Allocator
    alloc: A,
    /// 内部swiss table
    inner: RawTableInner,
}

impl<E: EntrySpec, A: Allocator> RawTable2<E, A> {
    ///
    /// 构造新的哈希表, 支持指定默认cap, 若为0则视为构造空的哈希表
    ///
    pub fn new(
        cap: usize,
        layout: EntryLayout,
        spec: E,
        alloc: A,
    ) -> Result<Self, TryReserveError> {
        let table_layout = TableLayout::from(layout);
        let inner = RawTableInner::fallible_with_capacity(
            &alloc,
            table_layout,
            cap,
            Fallibility::Fallible,
        )?;
        Ok(Self {
            layout,
            spec,
            alloc,
            inner,
        })
    }

    ///
    /// 获取`key: &K`在此map中的`value: &V`, 出入参均为K/V的有效内存指针。
    /// 它的使用场景为`map.get(key)`, 计算过程中直接使用`self.hash_fn`和`self.eq_fn`
    ///
    pub unsafe fn access(&self, key: *const u8) -> Option<*const u8> {
        let bucket = self.find(key).map(|o| self.bucket(o));
        bucket.map(|ptr| ptr.add(self.layout.voff as usize))
    }

    ///
    /// 获取`key: &K`在当前map中的“可赋值地址”, 出入参均为K/V的有效内存地址。
    ///
    /// 它的使用场景为`map.set(key, value)`, 只是过程分为两步走:
    /// 先计算key槽位物理地址, 然后向地址内写入value；此函数只负责第一步, 即按需扩容+返回value地址, 由caller写入数据
    ///
    pub unsafe fn assign(&mut self, key: *const u8) -> *const u8 {
        self.check_growth(1).expect("map growth failure");

        let index = self.find_or_insert(key);
        let ptr = self.bucket(index);
        // move pointer to value's offset
        ptr.add(self.layout.voff as usize)
    }

    ///
    /// 将other中的所有buckets导入当前map
    ///
    pub unsafe fn extend(&mut self, other: &Self) {
        self.check_growth(other.len()).expect("map growth failure");

        for other_idx in other.inner.full_buckets_indices() {
            let other_bucket = other.bucket(other_idx);
            let index = self.find_or_insert(other_bucket);
            let bucket = self.bucket(index);
            let v_ptr = bucket.add(self.layout.voff as usize);
            let other_v_ptr = other_bucket.add(self.layout.voff as usize);
            self.spec.assign_value(v_ptr, other_v_ptr);
        }
    }

    ///
    /// 从当前map中删除指定key, 即将该key对应的Bucket软删除
    ///
    pub unsafe fn delete(&mut self, key: *const u8) {
        self.find(key).map(|i| self.inner.erase(i));
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
    /// 针对当前table派生出`map<K, V>`的便捷封装, caller需要保证内存安全性
    ///
    pub unsafe fn as_map<K, V>(&'_ mut self) -> RawMap<'_, K, V, E, A> {
        RawMap {
            table: self,
            phantom: Default::default(),
        }
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
    unsafe fn bucket(&self, index: usize) -> *const u8 {
        self.inner.bucket_ptr(index, self.layout.size as usize)
    }

    #[inline(always)]
    unsafe fn find(&self, key: *const u8) -> Option<usize> {
        let hash = self.spec.hash(key);
        let mut equals = |index| self.spec.equals(key, self.bucket(index));
        self.inner.find_inner(hash, &mut equals)
    }

    #[inline(always)]
    unsafe fn find_or_insert(&mut self, key: *const u8) -> usize {
        let hash = self.spec.hash(key);
        let mut equals = |index| self.spec.equals(key, self.bucket(index));
        match self.inner.find_or_find_insert_slot_inner(hash, &mut equals) {
            Ok(index) => index,
            Err(slot) => {
                let old_ctrl = *self.inner.ctrl(slot.index);
                self.inner.record_item_insert_at(slot.index, old_ctrl, hash);
                let bucket = self.bucket(slot.index);
                self.spec.assign_key(bucket, key); // write key into slot
                slot.index
            }
        }
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
        let size = self.layout.size as usize;
        self.inner.reserve_rehash_inner(
            &self.alloc,
            additional,
            &|table, index| self.spec.hash(table.bucket_ptr(index, size)),
            fallibility,
            TableLayout::from(self.layout),
            None,
        )
    }
}

pub struct RawMap<'a, K, V, E: EntrySpec, A: Allocator> {
    table: &'a mut RawTable2<E, A>,
    phantom: PhantomData<(K, V)>,
}

impl<'a, K, V, E: EntrySpec, A: Allocator> RawMap<'a, K, V, E, A> {
    /// 获取此map中指定key的value引用
    pub unsafe fn get(&self, key: &K) -> Option<&V> {
        let key_ptr = key as *const K as *const u8;
        self.table.access(key_ptr).map(|ptr| &*(ptr as *const V))
    }

    /// 将{key, value}写入此map
    pub unsafe fn insert(&mut self, key: &K, value: V) {
        let key_ptr = key as *const K as *const u8;
        let val_addr = self.table.assign(key_ptr);
        let val_mut_ref = &mut *(val_addr as *mut V);
        *val_mut_ref = value;
    }

    /// 删除此map中指定key的entry
    pub unsafe fn delete(&mut self, key: &K) {
        let key_ptr = key as *const K as *const u8;
        self.table.delete(key_ptr);
    }

    /// Merge all entries of other into this map.
    pub unsafe fn extend(&mut self, other: &Self) {
        self.table.extend(&other.table);
    }

    /// Clear all entries in this map.
    pub unsafe fn clear(&mut self) {
        self.table.clear()
    }

    /// Obtains the count of entries in this map.
    pub fn size(&self) -> usize {
        self.table.len()
    }
}
