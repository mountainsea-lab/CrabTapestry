#[macro_export]
macro_rules! impl_full_repository {
    (
        $repo:ident,               // repository struct name
        $table:ident,              // table mod name (Diesel schema)
        $model:ty,                 // queried model
        $new_model:ty,             // new entity for insert
        $update_model:ty           // update entity for update
    ) => {
        impl<'a> Repository<$model> for $repo<'a> {
            fn get_all(&mut self) -> AppResult<Vec<$model>> {
                use crate::schema::$table::dsl::*;
                $table.select(<$model>::as_select()).load(self.conn).map_err(AppError::from)
            }

            fn get_by_id(&mut self, id: u64) -> AppResult<Option<$model>> {
                use crate::schema::$table::dsl::*;
                $table
                    .find(id)
                    .select(<$model>::as_select())
                    .first(self.conn)
                    .optional()
                    .map_err(AppError::from)
            }

            fn delete(&mut self, id: u64) -> AppResult<usize> {
                use crate::schema::$table::dsl::*;
                diesel::delete($table.find(id)).execute(self.conn).map_err(AppError::from)
            }
        }

        impl<'a> crate::domain::repository::InsertableRepository<$new_model> for $repo<'a> {
            fn insert(&mut self, entity: &$new_model) -> AppResult<usize> {
                use crate::schema::$table::dsl::*;
                diesel::insert_into($table)
                    .values(entity)
                    .execute(self.conn)
                    .map_err(AppError::from)
            }
        }

        impl<'a> crate::domain::repository::UpdatableRepository<$update_model> for $repo<'a> {
            fn update(&mut self, entity: &$update_model) -> AppResult<usize> {
                use crate::schema::$table::dsl::*;
                diesel::update($table.find(&entity.id))
                    .set(entity)
                    .execute(self.conn)
                    .map_err(AppError::from)
            }
        }
    };
}
