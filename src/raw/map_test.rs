use super::map::*;
use super::Global;
use std::alloc::Layout;

#[derive(Debug, Copy, Clone)]
pub struct Float64Key(Layout);

impl EntrySpec for Float64Key {
    #[inline]
    fn layout(&self) -> Layout {
        self.0
    }

    #[inline]
    fn hash(&self, ptr: *const u8) -> u64 {
        let p = unsafe { &*(ptr as *const f64) };
        match *p {
            0.0 => 0,
            -0.0 => 0,
            _ => p.to_bits(),
        }
    }

    #[inline]
    fn equals(&self, a: *const u8, b: *const u8) -> bool {
        let p1 = unsafe { &*(a as *const f64) };
        let p2 = unsafe { &*(b as *const f64) };
        p1 == p2
    }

    #[inline]
    fn access_value(&self, ptr: *const u8) -> *const u8 {
        unsafe { ptr.offset(8) }
    }

    #[inline]
    fn assign_key(&self, ptr: *const u8, v: *const u8) {
        let key_ref = unsafe { &mut *(ptr as *mut f64) };
        let key = unsafe { &*(v as *const f64) };
        *key_ref = *key;
    }
}

#[test]
fn test_map() {
    let entry = Float64Key(unsafe { Layout::from_size_align_unchecked(16, 8) });
    let mut table = RawTable2::new(0, entry, Global).expect("what?");
    let mut table2 = RawTable2::new(0, entry, Global).expect("what?");
    assert_eq!(table.len(), 0);

    unsafe {
        let mut map = table.as_map::<f64, f64>();
        let mut map2 = table2.as_map::<f64, f64>();
        // key不存在
        let key = 0.2233f64;
        assert!(map.get(&key).is_none());

        // 写入key
        let value = 1.23f64;
        map.insert(&key, value);

        // 重新读出
        assert_eq!(map.get(&key).unwrap(), &value);

        // 删除后验证
        map.delete(&key);
        assert!(map.get(&key).is_none());

        // 批量写入后清空
        for i in 0..10000 {
            map.insert(&(i as f64), i as f64);
        }
        assert_eq!(map.size(), 10000);
        map2.extend(&map);
        map.clear();
        assert_eq!(map.size(), 0);
        assert_eq!(map2.size(), 10000);
        
        // 针对map2迭代
    }
}
