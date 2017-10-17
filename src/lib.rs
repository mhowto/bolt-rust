mod bucket;
mod node;
mod types;
mod tx;
mod db;

#[cfg(test)]
mod tests {
    use node::Node;
    use std::sync::Arc;
    use bucket::Bucket;
    use bucket::_Bucket;
    use tx::Tx;
    use db::Meta;
    use std::str;

    #[test]
    fn it_works() {
        let mut node = Node::new(Arc::new(Bucket::new(Box::new(_Bucket {
                                                          root: 0,
                                                          sequence: 0,
                                                      }),
                                                      Arc::new(Tx { meta: Meta::new() }),
                                                      None)));
        node.put("baz".as_bytes(), "baz".as_bytes(), "2".as_bytes(), 0, 0);
        node.put("foo".as_bytes(), "foo".as_bytes(), "0".as_bytes(), 0, 0);
        node.put("bar".as_bytes(), "bar".as_bytes(), "1".as_bytes(), 0, 0);
        node.put("foo".as_bytes(), "foo".as_bytes(), "3".as_bytes(), 0, 0x02);

        assert_eq!(node.inodes.len(), 3);

        {

            let inode = &node.inodes[0];
            assert_eq!(str::from_utf8(inode.key).unwrap(), "bar");
            assert_eq!(str::from_utf8(inode.value).unwrap(), "1");
        }

        {

            let inode = &node.inodes[1];
            assert_eq!(str::from_utf8(inode.key).unwrap(), "baz");
            assert_eq!(str::from_utf8(inode.value).unwrap(), "2");
        }

        {
            let inode = &node.inodes[2];
            assert_eq!(str::from_utf8(inode.key).unwrap(), "foo");
            assert_eq!(str::from_utf8(inode.value).unwrap(), "3");
        }

        {
            assert_eq!(node.inodes[2].flags, 0x02);
        }
    }
}
