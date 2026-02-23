/// Returns true if the value is the default value for its type.
/// Used to skip serializing default values.
pub fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    value == &T::default()
}
