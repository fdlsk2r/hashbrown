use super::map::*;
use super::Global;

#[derive(Debug, Copy, Clone)]
pub struct Float64Spec();

impl EntrySpec for Float64Spec {
    unsafe fn hash(&self, ptr: *const u8) -> u64 {
        let p = &*(ptr as *const f64);
        match *p {
            0.0 => 0,
            -0.0 => 0,
            _ => p.to_bits(),
        }
    }

    unsafe fn equals(&self, k1: *const u8, k2: *const u8) -> bool {
        let p1 = &*(k1 as *const f64);
        let p2 = &*(k2 as *const f64);
        p1 == p2
    }

    unsafe fn assign_key(&self, k_ptr: *const u8, other: *const u8) {
        let key_ref = &mut *(k_ptr as *mut f64);
        let key = &*(other as *const f64);
        *key_ref = *key;
    }

    unsafe fn assign_value(&self, v_ptr: *const u8, other: *const u8) {
        let key_ref = &mut *(v_ptr as *mut f64);
        let key = &*(other as *const f64);
        *key_ref = *key;
    }
}

#[test]
fn test_map() {
    let entry = Float64Spec();
    let layout = EntryLayout::new(8, 8, 16);
    let mut table = RawTable2::new(0, layout, entry, Global).expect("what?");
    let mut table2 = RawTable2::new(0, layout, entry, Global).expect("what?");
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
