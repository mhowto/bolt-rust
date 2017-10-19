/// Macro to get the offset of a struct field in bytes from the address of the
/// struct.
///
/// This macro is identical to `offset_of!` but doesn't give a warning about
/// unnecessary unsafe blocks when invoked from unsafe code.
#[macro_export]
macro_rules! offset_of_unsafe {
    ($container:path, $field:ident) => {{
        // Make sure the field actually exists. This line ensures that a
        // compile-time error is generated if $field is accessed through a
        // Deref impl.
        let $container { $field : _, .. };

        // Create an instance of the container and calculate the offset to its
        // field. Although we are creating references to uninitialized data this
        // is fine since we are not dereferencing them.
        let val: $container = $crate::__core::mem::uninitialized();
        let result = &val.$field as *const _ as usize - &val as *const _ as usize;
        $crate::__core::mem::forget(val);
        result as isize
    }};
}

/// Macro to get the offset of a struct field in bytes from the address of the
/// struct.
///
/// This macro will cause a warning if it is invoked in an unsafe block. Use the
/// `offset_of_unsafe` macro instead to avoid this warning.
#[macro_export]
macro_rules! offset_of {
    ($container:path, $field:ident) => {
        unsafe { offset_of_unsafe!($container, $field) }
    };
}

/*
#[macro_export]
macro_rules! offset_of {
    ($father:ty, $($field:tt)+) => ({
        let root: $father = unsafe { ::std::mem::uninitialized() };

        let base = &root as *const _ as usize;
        let member =  &root.$($field)* as *const _ as usize;

        ::std::mem::forget(root);

        member - base
    });
}
*/