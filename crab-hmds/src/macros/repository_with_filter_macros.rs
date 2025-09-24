#[macro_export]
macro_rules! impl_repository_with_filter {
    (
        $repo:ident,
        $table:ident,
        $entity:ty,
        $filter:ty,
        @filter_var = $filter_var:ident,
        { $($body:tt)* }
    ) => {
        impl<'a> crate::domain::repository::FilterableRepository<$filter, $entity> for $repo<'a> {
            fn filter_paginated(
                &mut self,
                $filter_var: &$filter,
                page: i64,
                per_page: i64,
            ) -> AppResult<Vec<$entity>> {
                use diesel::prelude::*;

                let q = {
                            $($body)*
                          };

                q.limit(per_page)
                    .offset(page * per_page)
                    .load::<$entity>(self.conn)
                    .map_err(Into::into)
            }

            fn count_filtered(&mut self, $filter_var: &$filter) -> AppResult<i64> {
                use diesel::prelude::*;


               let q = {
                            $($body)*
                          };
                q.count()
                    .get_result::<i64>(self.conn)
                    .map_err(Into::into)
            }
        }
    };
}
