mod bucket;
mod node;
mod types;
mod tx;
mod db;

#[cfg(test)]
mod tests {
    use node::Node;
    use std::rc::Rc;
    use bucket::Bucket;
    use bucket::_Bucket;
    use std::sync::Arc;
    use std::ptr;
    use tx::Tx;
    use db::Meta;

    #[test]
    fn it_works() {
        let node = Node::new(Rc::new(Bucket::new(
            Box::new(_Bucket{root: 0, sequence: 0}),
            Arc::new(Tx{meta: Box::new(Meta::new())}),
            Arc::new(Node::new()),
        )));
        node.put()
    }
}
