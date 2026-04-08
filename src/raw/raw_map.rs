use super::alloc::{Allocator, Global};
use super::raw_table::*;
use crate::DefaultHashBuilder;
use core::hash::{BuildHasher, Hash, Hasher};
use core::marker::PhantomData;

/// The unsafe `Map<K, V>` mapping for [`RawTable`], used for test convenience.
pub struct RawMap<K: Hash + Eq, V, A: Allocator = Global> {
    table: RawTable<A>,
    hash_builder: DefaultHashBuilder,
    phantom: PhantomData<(K, V)>,
}

impl<K: Hash + Eq, V> RawMap<K, V, Global> {
    /// Create new `RawMap<K, V>` based on [`RawTable`], use Global::default as allocator.
    pub fn new(cap: usize) -> Self {
        let layout = core::alloc::Layout::new::<(K, V)>();
        RawMap {
            table: RawTable::new(cap, layout.into(), Global::default()).expect("heap overflow"),
            hash_builder: DefaultHashBuilder::default(),
            phantom: Default::default(),
        }
    }
}

impl<K: Hash + Eq, V, A: Allocator> RawMap<K, V, A> {
    /// Create new `RawMap<K, V>` based on [`RawTable`], use specified allocator.
    pub fn new_in(cap: usize, alloc: A) -> Self {
        let layout = core::alloc::Layout::new::<(K, V)>();
        RawMap {
            table: RawTable::new(cap, layout.into(), alloc).expect("heap overflow"),
            hash_builder: DefaultHashBuilder::default(),
            phantom: Default::default(),
        }
    }

    /// Obtain value's ref by specified key.
    ///
    /// Caller must make sure that this table's layout is `map<K, V>`.
    pub unsafe fn get(&self, key: &K) -> Option<&V> {
        let hash = self.do_hash(key);
        let slot = self
            .table
            .find(hash, |o| key.eq(&o.cast::<(K, V)>().as_ref().0))?;
        let entry = self.table.bucket(slot).cast::<(K, V)>().as_ref();
        Some(&entry.1)
    }

    /// Insert new entry(key, value) into current table.
    ///
    ///  Caller must make sure that this table's layout is `map<K, V>`.
    pub unsafe fn set(&mut self, key: K, value: V) {
        self.check_growth(1);

        // find insert/update bucket
        let hash = self.do_hash(&key);
        let (is_new, slot) = self
            .table
            .find_for_insert(hash, |o| key.eq(&o.cast::<(K, V)>().as_ref().0));

        // (K, V): Use rust's default layout
        let mut entry = self.table.bucket(slot).cast::<(K, V)>();
        match is_new {
            true => entry.write((key, value)),
            false => entry.as_mut().1 = value,
        };
    }

    /// Delete `Bucket<K, V>` from this map by the specified key.
    pub unsafe fn delete(&mut self, key: &K) -> bool {
        let hash = self.do_hash(key);
        let slot = match self
            .table
            .find(hash, |o| key.eq(&o.cast::<(K, V)>().as_ref().0))
        {
            None => return false,
            Some(slot) => slot,
        };
        self.table.bucket(slot).cast::<(K, V)>().drop_in_place();
        self.table.erase(slot);
        true
    }

    /// Merge all entries of other table into the current `Map<K, V>`.
    pub unsafe fn extend(&mut self, mut other: Self) {
        self.check_growth(other.table.len());

        // clone all entries into current table
        let mut it = RawTableIter::new_zeroed();
        while let Some(slot) = other.table.iter_next(&mut it) {
            let other_entry = other.table.bucket(slot).cast::<(K, V)>();
            // find the insert/update slot
            let key_ref = &other_entry.as_ref().0;
            let hash = self.do_hash(key_ref);
            let (is_new, insert_slot) = self
                .table
                .find_for_insert(hash, |o| key_ref.eq(&o.cast::<(K, V)>().as_ref().0));

            // take (K, V) from other, and execute insert/update operation
            let (key, value) = other_entry.read();
            other.table.erase(slot);
            let mut entry = self.table.bucket(insert_slot).cast::<(K, V)>();
            match is_new {
                true => entry.write((key, value)),
                false => entry.as_mut().1 = value,
            };
        }

        // mark other is an empty map, because all (K,V)'s ownership have been token
        other.table.clear();
    }

    /// 创建只读迭代器
    #[inline(always)]
    pub fn iter(&self) -> RawMapIter<'_, K, V, A> {
        RawMapIter {
            table: &self.table,
            iter: RawTableIter::new_zeroed(),
            phantom: PhantomData,
        }
    }

    /// 清空哈希表，遍历释放所有`Bucket<(K, V)>`的所有权
    #[inline(always)]
    pub fn clear(&mut self) {
        unsafe {
            let mut it = RawTableIter::new_zeroed();
            while let Some(slot) = self.table.iter_next(&mut it) {
                self.table.bucket(slot).cast::<(K, V)>().drop_in_place();
            }
        }
        self.table.clear();
    }

    ///
    #[inline(always)]
    pub fn size(&self) -> usize {
        self.table.len()
    }

    ///
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.size() == 0
    }

    #[inline(always)]
    fn do_hash(&self, key: &K) -> u64 {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[inline(always)]
    unsafe fn check_growth(&mut self, n: usize) {
        let builder = &self.hash_builder;
        self.table
            .check_growth(n, |o| {
                let mut hasher = builder.build_hasher();
                o.cast::<(K, V)>().as_ref().0.hash(&mut hasher);
                hasher.finish()
            })
            .expect("heap overflow");
    }
}

impl<K: Hash + Eq, V, A: Allocator> Drop for RawMap<K, V, A> {
    fn drop(&mut self) {
        self.clear();
    }
}

/// `RawMap<K, V>`迭代器
pub struct RawMapIter<'a, K, V, A: Allocator> {
    iter: RawTableIter,
    table: &'a RawTable<A>,
    phantom: PhantomData<(&'a K, &'a V)>,
}

impl<'a, K: Hash + Eq, V, A: Allocator> Iterator for RawMapIter<'a, K, V, A> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let slot = unsafe { self.table.iter_next(&mut self.iter) }?;
        let entry = unsafe { self.table.bucket(slot).cast::<(K, V)>().as_ref() };
        Some((&entry.0, &entry.1))
    }
}
