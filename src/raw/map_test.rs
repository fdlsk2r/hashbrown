use super::map::*;
use super::Global;
use std::alloc::Layout;

pub struct Float64Key();

impl KeyDesc for Float64Key {
    fn hash(&self, ptr: *const u8) -> u64 {
        let p = unsafe { &*(ptr as *const f64) };
        match *p {
            0.0 => 0,
            -0.0 => 0,
            _ => p.to_bits(),
        }
    }

    fn equals(&self, a: *const u8, b: *const u8) -> bool {
        let p1 = unsafe { &*(a as *const f64) };
        let p2 = unsafe { &*(b as *const f64) };
        p1 == p2
    }
}

#[test]
fn test_map() {
    let entry = Entry {
        key_spec: Float64Key(),
        key_size: 8,
        val_offset: 8,
        layout: unsafe { Layout::from_size_align_unchecked(16, 8) },
    };
    let mut map = RawMap::new(0, entry, Global).expect("what?");
    assert_eq!(map.len(), 0);

    // key不存在
    let key = 0.2233f64;
    let key_ref = &key;
    let key_addr = (&key) as *const f64 as *const u8;
    let v1 = map.access(key_addr);
    assert!(v1.is_none());

    // 写入key
    let value = 1.23f64;
    let bucket = map.assign(key_addr);
    let v_ptr = unsafe { &mut *(bucket as *mut f64) };
    *v_ptr = value;

    // 重新读出
    let v2 = map.access(key_addr);
    assert!(v2.is_some());
    let v_ptr = unsafe { &mut *(v2.unwrap().offset(8) as *mut f64) };
    assert_eq!(*v_ptr, value);
}
