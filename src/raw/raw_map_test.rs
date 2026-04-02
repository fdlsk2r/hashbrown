use super::raw_map::RawMap;
use std::collections::HashSet;
use std::rc::Rc;
use std::string::{String, ToString};

#[test]
fn test_basic_crud() {
    unsafe {
        let mut map = RawMap::<String, i32>::new(10);
        assert!(map.is_empty());
        assert_eq!(map.size(), 0);

        // 1. 测试 Insert
        map.set("a".to_string(), 1);
        map.set("b".to_string(), 2);
        assert_eq!(map.size(), 2);

        // 2. 测试 Get
        assert_eq!(map.get(&"a".to_string()), Some(&1));
        assert_eq!(map.get(&"b".to_string()), Some(&2));
        assert_eq!(map.get(&"c".to_string()), None);

        // 3. 测试 Update (覆盖写)
        map.set("a".to_string(), 100);
        assert_eq!(map.get(&"a".to_string()), Some(&100));
        assert_eq!(map.size(), 2); // size 不变

        // 4. 测试 Delete
        assert!(map.delete(&"a".to_string()));
        assert_eq!(map.get(&"a".to_string()), None);
        assert_eq!(map.size(), 1);

        // 重复删除
        assert!(!map.delete(&"a".to_string()));
    }
}

#[test]
fn test_memory_drop_on_clear_and_scope_exit() {
    let k1 = Rc::new("key1".to_string());
    let v1 = Rc::new("val1".to_string());
    let k2 = Rc::new("key2".to_string());
    let v2 = Rc::new("val2".to_string());

    {
        let mut map = RawMap::new(10);
        unsafe {
            map.set(k1.clone(), v1.clone());
            map.set(k2.clone(), v2.clone());
        }
        assert_eq!(Rc::strong_count(&k1), 2);
        assert_eq!(Rc::strong_count(&v1), 2);

        // 测试 clear 是否正确触发了 Drop
        map.clear();
        assert_eq!(Rc::strong_count(&k1), 1);
        assert_eq!(Rc::strong_count(&v1), 1);
        assert!(map.is_empty());

        // 再次放入，测试 map 离开作用域时的 Drop
        unsafe {
            map.set(k1.clone(), v1.clone());
        }
        assert_eq!(Rc::strong_count(&v1), 2);
    } // map 发生 Drop

    // 验证孤儿内存被完全回收
    assert_eq!(Rc::strong_count(&k1), 1);
    assert_eq!(Rc::strong_count(&v1), 1);
}

#[test]
fn test_memory_drop_on_overwrite() {
    let mut map = RawMap::new(10);
    let k = Rc::new("key".to_string());
    let v_old = Rc::new("val_old".to_string());
    let v_new = Rc::new("val_new".to_string());

    unsafe {
        map.set(k.clone(), v_old.clone());
        assert_eq!(Rc::strong_count(&v_old), 2);

        // 覆盖写：应当 Drop 旧的 Value，且传入的相同 Key 应当在使用后被安全 Drop
        let dup_k = k.clone();
        map.set(dup_k, v_new.clone());

        assert_eq!(Rc::strong_count(&v_old), 1, "Old value should be dropped");
        assert_eq!(
            Rc::strong_count(&k),
            2,
            "Dup key should be dropped, original kept"
        );
        assert_eq!(Rc::strong_count(&v_new), 2, "New value should be kept");
    }
}

#[test]
fn test_extend_and_ownership_transfer() {
    let rc_val = Rc::new(100);
    let mut map1 = RawMap::new(10);
    let mut map2 = RawMap::new(10);

    unsafe {
        map1.set(1, Rc::new(1));
        map1.set(2, Rc::new(2));

        // map2 包含冲突的 Key 2 和 新的 Key 3
        map2.set(2, rc_val.clone());
        map2.set(3, Rc::new(3));

        assert_eq!(Rc::strong_count(&rc_val), 2);

        // 执行扩展
        map1.extend(map2);

        assert_eq!(map1.size(), 3);
        assert_eq!(**map1.get(&1).unwrap(), 1);
        assert_eq!(
            **map1.get(&2).unwrap(),
            100,
            "Should overwrite with map2's value"
        );
        assert_eq!(**map1.get(&3).unwrap(), 3);

        // 验证 extend 期间，没有发生 Double Free，且所有权正确转移
        assert_eq!(Rc::strong_count(&rc_val), 2);
    }
}

#[test]
fn test_iterator() {
    let mut map = RawMap::new(10);
    unsafe {
        map.set("a", 1);
        map.set("b", 2);
        map.set("c", 3);
    }

    let mut extracted = HashSet::new();
    for (k, v) in map.iter() {
        extracted.insert((*k, *v));
    }

    assert_eq!(extracted.len(), 3);
    assert!(extracted.contains(&("a", 1)));
    assert!(extracted.contains(&("b", 2)));
    assert!(extracted.contains(&("c", 3)));
}

#[test]
fn test_rehashing_and_growth() {
    let mut map = RawMap::new(2); // 初始容量极小

    // 插入大量元素触发强制扩容 (Rehash)
    for i in 0..1000 {
        unsafe {
            map.set(i, i * 10);
        }
    }

    assert_eq!(map.size(), 1000);
    for i in 0..1000 {
        unsafe {
            assert_eq!(map.get(&i), Some(&(i * 10)));
        }
    }
}

#[test]
fn test_zero_sized_type_value() {
    // 测试 Set / HashSet 常用的 ZST 模式
    let mut map = RawMap::<i32, ()>::new(10);
    unsafe {
        map.set(1, ());
        map.set(2, ());
    }

    assert_eq!(map.size(), 2);
    unsafe {
        assert_eq!(map.get(&1), Some(&()));
        assert_eq!(map.get(&3), None);
    }
}
