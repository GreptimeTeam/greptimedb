use crate::schema::ProjectedSchema;

/// Hepler struct to do projection.
#[derive(Debug)]
struct Projector {
    projected_schema: ProjectedSchema,
    //
}

impl Projector {
    fn new(projected_schema: ProjectedSchema) -> Projector {
        Projector { projected_schema }
    }
}
