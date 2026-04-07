use super::{
    likely, Allocator, Fallibility, Global, Group, RawTableInner, TableLayout, TryReserveError,
};
use crate::control::{BitMaskIter, Tag};
use core::ptr::NonNull;

/// 参考[`TableLayout::new`]函数实现的，Layout转换函数。
///
/// 使用方需要提供标准的`Layout`来描述哈希表中`Bucket, Entry, (K, V)`的内存结构。
impl From<core::alloc::Layout> for TableLayout {
    fn from(value: core::alloc::Layout) -> Self {
        Self {
            size: value.size(),
            ctrl_align: usize::max(value.align(), Group::WIDTH),
        }
    }
}

/// 底层swiss-table的槽位迭代器，负责跟踪迭代状态。
///
/// 内存布局: 严格采用`8B align + 16B size`, 即可以绝对安全地采用`[0u64; 2]`作为初始迭代状态。
#[repr(C)]
#[repr(align(8))]
pub struct RawTableIter {
    /// 下一个将要加载的`Group`的起始索引
    next_group_index: u64,
    /// 正在消耗的`Group`的有效位图, 在不同的平台上，其可能是`u16/u64`。
    current_bitmask: BitMaskIter,
}

impl Default for RawTableIter {
    fn default() -> Self {
        Self::new_zeroed()
    }
}

impl RawTableIter {
    const _ASSERT_SIZE: () = assert!(
        size_of::<Self>() == 16,
        "RawTable2Iter MUST be exactly 16 bytes for VM stack compatibility"
    );
    const _ASSERT_ALIGN: () = assert!(
        align_of::<Self>() == 8,
        "RawTable2Iter MUST be 8-byte aligned"
    );

    /// 创建新迭代器
    #[inline(always)]
    pub const fn new_zeroed() -> Self {
        unsafe { core::mem::transmute([0u64; 2]) }
    }
}

///
/// The underline raw-memory swiss-table.
///
pub struct RawTable<A: Allocator = Global> {
    alloc: A,
    inner: RawTableInner,
    layout: TableLayout,
}

impl<A: Allocator> RawTable<A> {
    ///
    /// 构造新的哈希表, 支持指定默认cap, 若为0则视为构造空的哈希表
    ///
    pub fn new(cap: usize, layout: core::alloc::Layout, alloc: A) -> Result<Self, TryReserveError> {
        let layout = TableLayout::from(layout);
        let inner =
            RawTableInner::fallible_with_capacity(&alloc, layout, cap, Fallibility::Fallible)?;
        Ok(Self {
            layout,
            alloc,
            inner,
        })
    }

    /// 查找Key(hash + equal_fn)在此哈希表内的槽位。
    ///
    /// 适用场景：`get(key), access(key)`
    #[inline(always)]
    pub unsafe fn find(&self, hash: u64, equal_fn: impl Fn(NonNull<u8>) -> bool) -> Option<usize> {
        self.inner.find_inner(
            hash,                                  // 搜索哈希值
            &mut |idx| equal_fn(self.bucket(idx)), // 哈希相同时的equal回调
        )
    }

    /// 查找或创建Key(hash + equal_fn)在此哈希表内的槽位。
    ///
    /// 适用场景：`update(old), insert(new), assign(new), extend(other_map)`，
    /// 调用方需要额外调用`self.bucket`拿到槽位指针，并完成`(K, V)`数据写入。
    #[inline(always)]
    pub unsafe fn find_for_insert(
        &mut self,
        hash: u64,
        equal_fn: impl Fn(NonNull<u8>) -> bool,
    ) -> (bool, usize) {
        debug_assert!(
            self.inner.growth_left > 0,
            "table is full, call check_growth first"
        );
        match self.inner.find_or_find_insert_index_inner(
            hash,                                  // 搜索哈希值
            &mut |idx| equal_fn(self.bucket(idx)), // 哈希相同时的equal回调
        ) {
            Ok(old_slot) => (false, old_slot),
            Err(insert) => {
                let old_ctrl = *self.inner.ctrl(insert);
                let new_ctrl = Tag::full(hash);
                self.inner.record_item_insert_at(insert, old_ctrl, new_ctrl);
                (true, insert)
            }
        }
    }

    /// 获取指定index的Bucket内存指针。
    ///
    /// 入参index必须是来自于`find, find_for_insert`函数返回值的有效槽位。
    #[inline(always)]
    pub unsafe fn bucket(&self, index: usize) -> NonNull<u8> {
        let bucket_size = self.layout.size;
        NonNull::new_unchecked(self.inner.bucket_ptr(index, bucket_size))
    }

    /// 删除指定槽位数据，这里只抹去ctrl状态，具体(K, V)数据清理由caller负责。
    ///
    /// 适用场景: 搭配`find`函数可实现`delete(key)`功能。
    #[inline(always)]
    pub unsafe fn erase(&mut self, idx: usize) {
        self.inner.erase(idx);
    }

    /// 扫描底层buckets数组中`[index, size)`的下一个有效槽位。
    ///
    /// 可通过此函数在`[0, len)`的一维稀疏buckets数组中遍历FULL槽位，由caller递增index，从而实现无状态迭代。
    pub unsafe fn scan_next(&self, index: usize) -> Option<usize> {
        let buckets = self.inner.buckets();
        if index >= buckets {
            return None;
        }
        // 可能未内存对齐的首轮探测
        let group = unsafe { Group::load(self.inner.ctrl(index)) };
        let mask = group.match_full();
        if mask.any_bit_set() {
            let filled_idx = index + mask.trailing_zeros();
            return (filled_idx < buckets).then(|| filled_idx);
        }
        // 内存对齐的全局扫描，充分利用SIMD以优化性能
        let mut next_aligned = (index + Group::WIDTH) & !(Group::WIDTH - 1);
        while next_aligned < buckets {
            let group = unsafe { Group::load(self.inner.ctrl(next_aligned)) };
            let mask = group.match_full();
            if mask.any_bit_set() {
                let filled_idx = next_aligned + mask.trailing_zeros();
                return (filled_idx < buckets).then(|| filled_idx);
            }
            next_aligned += Group::WIDTH;
        }
        None
    }

    /// Advance the Iter, find and return the next Bucket's raw pointer.
    pub unsafe fn iter_next(&self, cursor: &mut RawTableIter) -> Option<usize> {
        loop {
            if let Some(bit) = cursor.current_bitmask.next() {
                let base_index = (cursor.next_group_index as usize) - Group::WIDTH;
                return Some(base_index + bit);
            }
            if (cursor.next_group_index as usize) >= self.inner.buckets() {
                return None;
            }
            cursor.current_bitmask = unsafe {
                let group_ptr = self.inner.ctrl(cursor.next_group_index as usize);
                Group::load_aligned(group_ptr).match_full().into_iter()
            };
            cursor.next_group_index += Group::WIDTH as u64;
        }
    }

    /// 检查底层swiss-table是否需要扩容。
    ///
    /// 适用场景: 在`put(new), extend(other_map)`场景中，用于执行底层buckets的预扩容。
    #[inline(always)]
    pub unsafe fn check_growth(
        &mut self,
        additional: usize,
        hash_fn: impl Fn(NonNull<u8>) -> u64,
    ) -> Result<(), TryReserveError> {
        if likely(additional <= self.inner.growth_left) {
            return Ok(());
        }
        let size = self.layout.size;
        // 执行扩容，RawTableInner强制要求通过入参来提供hasher函数，它应该会被编译器内联优化吧。
        self.inner.reserve_rehash_inner(
            &self.alloc,
            additional,
            &|t, idx| hash_fn(unsafe { NonNull::new_unchecked(t.bucket_ptr(idx, size)) }),
            Fallibility::Fallible,
            TableLayout::from(self.layout),
            None,
        )
    }

    /// 获取此哈希表中的单个Bucket的内存大小
    pub fn bucket_size(&self) -> usize {
        self.layout.size
    }

    /// 清空此哈希表中的所有buckets, 不释放内存
    pub fn clear(&mut self) {
        self.inner.clear_no_drop();
    }

    /// 获取此哈希表中的buckets数量
    pub fn len(&self) -> usize {
        self.inner.items
    }
}

impl<A: Allocator> Drop for RawTable<A> {
    #[cfg_attr(feature = "inline-more", inline)]
    fn drop(&mut self) {
        unsafe {
            self.inner.free_buckets(&self.alloc, self.layout.into());
        }
    }
}
