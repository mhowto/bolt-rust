mod bucket;
mod node;
mod types;

#[cfg(test)]
mod tests {
    use node::Node;
    use std::rc::Rc;
    use bucket::Bucket;

    #[test]
    fn it_works() {
        let node = Node::new(Rc::new(Bucket{}));
        node.put()
    }
}
