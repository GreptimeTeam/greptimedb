use api::v1::*;

#[derive(Clone, Debug)]
pub struct Admin {
    client: Client,
}

impl Admin {
    pub fn new(client: Client) -> Self {
        Self { client }
    }

    // TODO(jiachun): admin api
}
