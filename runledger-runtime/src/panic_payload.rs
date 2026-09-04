use std::any::Any;

pub(crate) fn panic_payload_message(panic_payload: &(dyn Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = panic_payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }

    "non-string panic payload".to_string()
}

#[cfg(test)]
mod tests {
    use super::panic_payload_message;

    #[test]
    fn preserves_owned_string_payload() {
        let payload = "owned panic".to_owned();

        assert_eq!(panic_payload_message(&payload), "owned panic");
    }

    #[test]
    fn preserves_static_string_payload() {
        let payload: &'static str = "static panic";

        assert_eq!(panic_payload_message(&payload), "static panic");
    }

    #[test]
    fn normalizes_non_string_payload() {
        let payload = 42_u32;

        assert_eq!(panic_payload_message(&payload), "non-string panic payload");
    }
}
