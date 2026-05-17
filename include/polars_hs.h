#ifndef POLARS_HS_H
#define POLARS_HS_H

#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#define PHS_OK 0

#define PHS_POLARS_ERROR 1

#define PHS_INVALID_ARGUMENT 2

#define PHS_UTF8_ERROR 3

#define PHS_PANIC 4

typedef struct phs_dataframe {
  uint8_t _private[0];
} phs_dataframe;

typedef struct phs_arrow_record_batch {
  uint8_t _private[0];
} phs_arrow_record_batch;

typedef struct phs_error {
  uint8_t _private[0];
} phs_error;

typedef struct phs_series {
  uint8_t _private[0];
} phs_series;

typedef struct phs_arrow_series {
  uint8_t _private[0];
} phs_arrow_series;

typedef struct phs_bytes {
  uint8_t _private[0];
} phs_bytes;

typedef struct phs_expr {
  uint8_t _private[0];
} phs_expr;

typedef struct phs_lazyframe {
  uint8_t _private[0];
} phs_lazyframe;

uint32_t phs_version_major(void);

uint32_t phs_version_minor(void);

int phs_dataframe_to_arrow_record_batch(const struct phs_dataframe *dataframe,
                                        struct phs_arrow_record_batch **out,
                                        struct phs_error **err);

void *phs_arrow_record_batch_schema(struct phs_arrow_record_batch *batch);

void *phs_arrow_record_batch_array(struct phs_arrow_record_batch *batch);

void phs_arrow_record_batch_free(struct phs_arrow_record_batch *batch);

int phs_series_to_arrow_array(const struct phs_series *series,
                              struct phs_arrow_series **out,
                              struct phs_error **err);

void *phs_arrow_series_schema(struct phs_arrow_series *series);

void *phs_arrow_series_array(struct phs_arrow_series *series);

void phs_arrow_series_free(struct phs_arrow_series *series);

int phs_series_from_arrow_array(void *schema,
                                void *array,
                                struct phs_series **out,
                                struct phs_error **err);

int phs_dataframe_from_arrow_record_batch(void *schema,
                                          void *array,
                                          struct phs_dataframe **out,
                                          struct phs_error **err);

uintptr_t phs_bytes_len(const struct phs_bytes *ptr);

const unsigned char *phs_bytes_data(const struct phs_bytes *ptr);

void phs_bytes_free(struct phs_bytes *ptr);

int phs_read_csv(const char *path, struct phs_dataframe **out, struct phs_error **err);

int phs_read_csv_options(const char *path,
                         bool has_header,
                         unsigned char separator,
                         bool has_null_value,
                         const char *null_value,
                         bool has_n_rows,
                         uint64_t n_rows,
                         uint64_t skip_rows,
                         uint64_t skip_rows_after_header,
                         bool has_infer_schema_length,
                         uint64_t infer_schema_length,
                         bool ignore_errors,
                         bool truncate_ragged_lines,
                         bool missing_is_null,
                         bool low_memory,
                         bool rechunk,
                         struct phs_dataframe **out,
                         struct phs_error **err);

int phs_read_parquet(const char *path, struct phs_dataframe **out, struct phs_error **err);

int phs_read_parquet_options(const char *path,
                             bool has_n_rows,
                             uint64_t n_rows,
                             int parallel,
                             bool low_memory,
                             bool rechunk,
                             struct phs_dataframe **out,
                             struct phs_error **err);

int phs_write_csv(const char *path, const struct phs_dataframe *dataframe, struct phs_error **err);

int phs_write_csv_options(const char *path,
                          const struct phs_dataframe *dataframe,
                          bool include_header,
                          unsigned char separator,
                          const char *null_value,
                          struct phs_error **err);

int phs_write_parquet(const char *path,
                      const struct phs_dataframe *dataframe,
                      struct phs_error **err);

int phs_write_parquet_options(const char *path,
                              const struct phs_dataframe *dataframe,
                              int compression,
                              bool has_row_group_size,
                              uint64_t row_group_size,
                              bool has_data_page_size,
                              uint64_t data_page_size,
                              bool statistics_min_value,
                              bool statistics_max_value,
                              bool statistics_distinct_count,
                              bool statistics_null_count,
                              bool parallel,
                              struct phs_error **err);

int phs_dataframe_new(const struct phs_series *const *series,
                      uintptr_t len,
                      struct phs_dataframe **out,
                      struct phs_error **err);

int phs_dataframe_select(const struct phs_dataframe *dataframe,
                         const char *const *names,
                         uintptr_t len,
                         struct phs_dataframe **out,
                         struct phs_error **err);

int phs_dataframe_drop(const struct phs_dataframe *dataframe,
                       const char *const *names,
                       uintptr_t len,
                       struct phs_dataframe **out,
                       struct phs_error **err);

int phs_dataframe_rename(const struct phs_dataframe *dataframe,
                         const char *const *existing,
                         const char *const *new_names,
                         uintptr_t len,
                         struct phs_dataframe **out,
                         struct phs_error **err);

int phs_dataframe_slice(const struct phs_dataframe *dataframe,
                        int64_t offset,
                        uint64_t len,
                        struct phs_dataframe **out,
                        struct phs_error **err);

int phs_dataframe_filter(const struct phs_dataframe *dataframe,
                         const struct phs_series *mask,
                         struct phs_dataframe **out,
                         struct phs_error **err);

int phs_dataframe_take(const struct phs_dataframe *dataframe,
                       const uint64_t *indices,
                       uintptr_t len,
                       struct phs_dataframe **out,
                       struct phs_error **err);

int phs_dataframe_join(const struct phs_dataframe *left,
                       const struct phs_dataframe *right,
                       const char *const *left_on,
                       uintptr_t left_len,
                       const char *const *right_on,
                       uintptr_t right_len,
                       int join_type,
                       const char *suffix,
                       struct phs_dataframe **out,
                       struct phs_error **err);

int phs_dataframe_vstack(const struct phs_dataframe *left,
                         const struct phs_dataframe *right,
                         struct phs_dataframe **out,
                         struct phs_error **err);

int phs_dataframe_hstack(const struct phs_dataframe *dataframe,
                         const struct phs_series *const *series,
                         uintptr_t len,
                         struct phs_dataframe **out,
                         struct phs_error **err);

int phs_dataframe_with_columns(const struct phs_dataframe *dataframe,
                               const struct phs_series *const *series,
                               uintptr_t len,
                               struct phs_dataframe **out,
                               struct phs_error **err);

int phs_dataframe_fill_null(const struct phs_dataframe *dataframe,
                            int strategy,
                            bool has_limit,
                            uint64_t limit,
                            struct phs_dataframe **out,
                            struct phs_error **err);

int phs_dataframe_sort(const struct phs_dataframe *dataframe,
                       const char *const *names,
                       uintptr_t names_len,
                       const uint8_t *descending,
                       uintptr_t descending_len,
                       const uint8_t *nulls_last,
                       uintptr_t nulls_last_len,
                       bool multithreaded,
                       bool maintain_order,
                       bool has_limit,
                       uint64_t limit,
                       struct phs_dataframe **out,
                       struct phs_error **err);

int phs_dataframe_unique(const struct phs_dataframe *dataframe,
                         const char *const *subset,
                         uintptr_t subset_len,
                         bool has_subset,
                         int keep_strategy,
                         bool maintain_order,
                         struct phs_dataframe **out,
                         struct phs_error **err);

int phs_dataframe_reverse(const struct phs_dataframe *dataframe,
                          struct phs_dataframe **out,
                          struct phs_error **err);

int phs_dataframe_drop_nulls(const struct phs_dataframe *dataframe,
                             const char *const *names,
                             uintptr_t len,
                             bool has_subset,
                             struct phs_dataframe **out,
                             struct phs_error **err);

int phs_dataframe_null_count(const struct phs_dataframe *dataframe,
                             struct phs_dataframe **out,
                             struct phs_error **err);

int phs_dataframe_shape(const struct phs_dataframe *dataframe,
                        uint64_t *height_out,
                        uint64_t *width_out,
                        struct phs_error **err);

int phs_dataframe_height(const struct phs_dataframe *dataframe,
                         uint64_t *height_out,
                         struct phs_error **err);

int phs_dataframe_width(const struct phs_dataframe *dataframe,
                        uint64_t *width_out,
                        struct phs_error **err);

int phs_dataframe_schema(const struct phs_dataframe *dataframe,
                         struct phs_bytes **out,
                         struct phs_error **err);

int phs_dataframe_head(const struct phs_dataframe *dataframe,
                       uint64_t n,
                       struct phs_dataframe **out,
                       struct phs_error **err);

int phs_dataframe_tail(const struct phs_dataframe *dataframe,
                       uint64_t n,
                       struct phs_dataframe **out,
                       struct phs_error **err);

int phs_dataframe_to_text(const struct phs_dataframe *dataframe,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_dataframe_column(const struct phs_dataframe *dataframe,
                         const char *name,
                         struct phs_series **out,
                         struct phs_error **err);

int phs_dataframe_column_bool(const struct phs_dataframe *dataframe,
                              const char *name,
                              struct phs_bytes **out,
                              struct phs_error **err);

int phs_dataframe_column_i64(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_i8(const struct phs_dataframe *dataframe,
                            const char *name,
                            struct phs_bytes **out,
                            struct phs_error **err);

int phs_dataframe_column_i16(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_i32(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_u8(const struct phs_dataframe *dataframe,
                            const char *name,
                            struct phs_bytes **out,
                            struct phs_error **err);

int phs_dataframe_column_u16(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_u32(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_u64(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_f32(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_f64(const struct phs_dataframe *dataframe,
                             const char *name,
                             struct phs_bytes **out,
                             struct phs_error **err);

int phs_dataframe_column_text(const struct phs_dataframe *dataframe,
                              const char *name,
                              struct phs_bytes **out,
                              struct phs_error **err);

int phs_error_code(const struct phs_error *error);

const char *phs_error_message(const struct phs_error *error);

void phs_error_free(struct phs_error *error);

int phs_expr_col(const char *name, struct phs_expr **out, struct phs_error **err);

int phs_expr_lit_bool(bool value, struct phs_expr **out, struct phs_error **err);

int phs_expr_lit_int(int64_t value, struct phs_expr **out, struct phs_error **err);

int phs_expr_lit_double(double value, struct phs_expr **out, struct phs_error **err);

int phs_expr_lit_text(const char *value, struct phs_expr **out, struct phs_error **err);

int phs_expr_alias(const struct phs_expr *expr,
                   const char *name,
                   struct phs_expr **out,
                   struct phs_error **err);

int phs_expr_not(const struct phs_expr *expr, struct phs_expr **out, struct phs_error **err);

int phs_expr_binary(int op,
                    const struct phs_expr *left,
                    const struct phs_expr *right,
                    struct phs_expr **out,
                    struct phs_error **err);

int phs_expr_agg(int op,
                 const struct phs_expr *expr,
                 struct phs_expr **out,
                 struct phs_error **err);

int phs_expr_cast(bool strict,
                  int dtype_code,
                  const struct phs_expr *expr,
                  struct phs_expr **out,
                  struct phs_error **err);

int phs_expr_unary(int op,
                   const struct phs_expr *expr,
                   struct phs_expr **out,
                   struct phs_error **err);

int phs_expr_unary_i64(int op,
                       int64_t arg,
                       const struct phs_expr *expr,
                       struct phs_expr **out,
                       struct phs_error **err);

int phs_expr_binary_function(int op,
                             const struct phs_expr *left,
                             const struct phs_expr *right,
                             struct phs_expr **out,
                             struct phs_error **err);

int phs_expr_ternary(const struct phs_expr *predicate,
                     const struct phs_expr *truthy,
                     const struct phs_expr *falsy,
                     struct phs_expr **out,
                     struct phs_error **err);

int phs_expr_quantile(int method,
                      const struct phs_expr *quantile,
                      const struct phs_expr *expr,
                      struct phs_expr **out,
                      struct phs_error **err);

int phs_expr_rank(int method,
                  bool descending,
                  const struct phs_expr *expr,
                  struct phs_expr **out,
                  struct phs_error **err);

int phs_expr_slice(const struct phs_expr *expr,
                   const struct phs_expr *offset,
                   const struct phs_expr *length,
                   struct phs_expr **out,
                   struct phs_error **err);

int phs_expr_sort_by(const struct phs_expr *expr,
                     const struct phs_expr *const *by,
                     uintptr_t by_len,
                     bool descending,
                     bool nulls_last,
                     bool multithreaded,
                     bool maintain_order,
                     struct phs_expr **out,
                     struct phs_error **err);

int phs_expr_over(const struct phs_expr *expr,
                  const struct phs_expr *const *partition_by,
                  uintptr_t partition_len,
                  struct phs_expr **out,
                  struct phs_error **err);

int phs_expr_string_function(int op,
                             const struct phs_expr *expr,
                             const struct phs_expr *const *args,
                             uintptr_t arg_len,
                             struct phs_expr **out,
                             struct phs_error **err);

int phs_expr_string_function_i64(int op,
                                 int64_t arg,
                                 const struct phs_expr *expr,
                                 const struct phs_expr *const *args,
                                 uintptr_t arg_len,
                                 struct phs_expr **out,
                                 struct phs_error **err);

int phs_expr_list_function(int op,
                           const struct phs_expr *expr,
                           const struct phs_expr *const *args,
                           uintptr_t arg_len,
                           struct phs_expr **out,
                           struct phs_error **err);

int phs_expr_temporal_function(int op,
                               const struct phs_expr *expr,
                               struct phs_expr **out,
                               struct phs_error **err);

int phs_expr_temporal_time_unit(int op,
                                int time_unit,
                                const struct phs_expr *expr,
                                struct phs_expr **out,
                                struct phs_error **err);

int phs_expr_temporal_string(int op,
                             const char *value,
                             const struct phs_expr *expr,
                             struct phs_expr **out,
                             struct phs_error **err);

int phs_expr_boolean_unary(int op,
                           const struct phs_expr *expr,
                           struct phs_expr **out,
                           struct phs_error **err);

int phs_expr_is_between(int closed,
                        const struct phs_expr *expr,
                        const struct phs_expr *lower,
                        const struct phs_expr *upper,
                        struct phs_expr **out,
                        struct phs_error **err);

int phs_expr_is_close(double abs_tol,
                      double rel_tol,
                      bool nans_equal,
                      const struct phs_expr *expr,
                      const struct phs_expr *other,
                      struct phs_expr **out,
                      struct phs_error **err);

int phs_expr_is_in(bool nulls_equal,
                   const struct phs_expr *expr,
                   const struct phs_expr *other,
                   struct phs_expr **out,
                   struct phs_error **err);

int phs_expr_clip(int op,
                  const struct phs_expr *expr,
                  const struct phs_expr *const *args,
                  uintptr_t arg_len,
                  struct phs_expr **out,
                  struct phs_error **err);

int phs_expr_horizontal_function(int op,
                                 bool flag,
                                 const struct phs_expr *const *exprs,
                                 uintptr_t len,
                                 struct phs_expr **out,
                                 struct phs_error **err);

int phs_expr_name_function(int op,
                           const char *first,
                           const char *second,
                           bool flag,
                           const struct phs_expr *expr,
                           struct phs_expr **out,
                           struct phs_error **err);

int phs_expr_string_nary_function(int op,
                                  const char *text,
                                  bool flag,
                                  const struct phs_expr *const *exprs,
                                  uintptr_t len,
                                  struct phs_expr **out,
                                  struct phs_error **err);

void phs_dataframe_free(struct phs_dataframe *ptr);

void phs_lazyframe_free(struct phs_lazyframe *ptr);

void phs_expr_free(struct phs_expr *ptr);

void phs_series_free(struct phs_series *ptr);

int phs_dataframe_to_ipc_bytes(const struct phs_dataframe *dataframe,
                               struct phs_bytes **out,
                               struct phs_error **err);

int phs_dataframe_from_ipc_bytes(const unsigned char *data,
                                 uintptr_t len,
                                 struct phs_dataframe **out,
                                 struct phs_error **err);

int phs_read_ipc_file(const char *path, struct phs_dataframe **out, struct phs_error **err);

int phs_write_ipc_file(const char *path,
                       const struct phs_dataframe *dataframe,
                       struct phs_error **err);

int phs_scan_csv(const char *path, struct phs_lazyframe **out, struct phs_error **err);

int phs_scan_csv_options(const char *path,
                         bool has_header,
                         unsigned char separator,
                         bool has_null_value,
                         const char *null_value,
                         bool has_n_rows,
                         uint64_t n_rows,
                         uint64_t skip_rows,
                         uint64_t skip_rows_after_header,
                         bool has_infer_schema_length,
                         uint64_t infer_schema_length,
                         bool ignore_errors,
                         bool truncate_ragged_lines,
                         bool missing_is_null,
                         bool low_memory,
                         bool rechunk,
                         struct phs_lazyframe **out,
                         struct phs_error **err);

int phs_scan_parquet(const char *path, struct phs_lazyframe **out, struct phs_error **err);

int phs_scan_parquet_options(const char *path,
                             bool has_n_rows,
                             uint64_t n_rows,
                             int parallel,
                             bool use_statistics,
                             bool low_memory,
                             bool rechunk,
                             bool cache,
                             struct phs_lazyframe **out,
                             struct phs_error **err);

int phs_lazyframe_collect(const struct phs_lazyframe *lazyframe,
                          struct phs_dataframe **out,
                          struct phs_error **err);

int phs_lazyframe_explain(const struct phs_lazyframe *lazyframe,
                          bool optimized,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_lazyframe_profile(const struct phs_lazyframe *lazyframe,
                          struct phs_dataframe **result_out,
                          struct phs_dataframe **profile_out,
                          struct phs_error **err);

int phs_lazyframe_filter(const struct phs_lazyframe *lazyframe,
                         const struct phs_expr *predicate,
                         struct phs_lazyframe **out,
                         struct phs_error **err);

int phs_lazyframe_select(const struct phs_lazyframe *lazyframe,
                         const struct phs_expr *const *exprs,
                         uintptr_t len,
                         struct phs_lazyframe **out,
                         struct phs_error **err);

int phs_lazyframe_with_columns(const struct phs_lazyframe *lazyframe,
                               const struct phs_expr *const *exprs,
                               uintptr_t len,
                               struct phs_lazyframe **out,
                               struct phs_error **err);

int phs_lazyframe_sort(const struct phs_lazyframe *lazyframe,
                       const char *const *names,
                       uintptr_t len,
                       struct phs_lazyframe **out,
                       struct phs_error **err);

int phs_lazyframe_limit(const struct phs_lazyframe *lazyframe,
                        uint64_t n,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_drop(const struct phs_lazyframe *lazyframe,
                       const char *const *names,
                       uintptr_t len,
                       struct phs_lazyframe **out,
                       struct phs_error **err);

int phs_lazyframe_rename(const struct phs_lazyframe *lazyframe,
                         const char *const *existing,
                         const char *const *new_,
                         uintptr_t len,
                         bool strict,
                         struct phs_lazyframe **out,
                         struct phs_error **err);

int phs_lazyframe_slice(const struct phs_lazyframe *lazyframe,
                        int64_t offset,
                        uint64_t len,
                        struct phs_lazyframe **out,
                        struct phs_error **err);

int phs_lazyframe_head(const struct phs_lazyframe *lazyframe,
                       uint64_t n,
                       struct phs_lazyframe **out,
                       struct phs_error **err);

int phs_lazyframe_tail(const struct phs_lazyframe *lazyframe,
                       uint64_t n,
                       struct phs_lazyframe **out,
                       struct phs_error **err);

int phs_lazyframe_drop_nulls(const struct phs_lazyframe *lazyframe,
                             const char *const *names,
                             uintptr_t len,
                             bool has_subset,
                             struct phs_lazyframe **out,
                             struct phs_error **err);

int phs_lazyframe_fill_null(const struct phs_lazyframe *lazyframe,
                            const struct phs_expr *value,
                            struct phs_lazyframe **out,
                            struct phs_error **err);

int phs_lazyframe_fill_nan(const struct phs_lazyframe *lazyframe,
                           const struct phs_expr *value,
                           struct phs_lazyframe **out,
                           struct phs_error **err);

int phs_lazyframe_null_count(const struct phs_lazyframe *lazyframe,
                             struct phs_lazyframe **out,
                             struct phs_error **err);

int phs_lazyframe_unique(const struct phs_lazyframe *lazyframe,
                         const char *const *names,
                         uintptr_t len,
                         bool has_subset,
                         int keep_strategy,
                         bool maintain_order,
                         struct phs_lazyframe **out,
                         struct phs_error **err);

int phs_lazyframe_group_by_agg(const struct phs_lazyframe *lazyframe,
                               const struct phs_expr *const *keys,
                               uintptr_t key_len,
                               const struct phs_expr *const *aggs,
                               uintptr_t agg_len,
                               bool maintain_order,
                               struct phs_lazyframe **out,
                               struct phs_error **err);

int phs_lazyframe_join(const struct phs_lazyframe *left,
                       const struct phs_lazyframe *right,
                       const struct phs_expr *const *left_on,
                       uintptr_t left_len,
                       const struct phs_expr *const *right_on,
                       uintptr_t right_len,
                       int join_type,
                       const char *suffix,
                       struct phs_lazyframe **out,
                       struct phs_error **err);

int phs_series_new_bool(const char *name,
                        const uint8_t *data,
                        uintptr_t len,
                        struct phs_series **out,
                        struct phs_error **err);

int phs_series_new_i64(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_i8(const char *name,
                      const uint8_t *data,
                      uintptr_t len,
                      struct phs_series **out,
                      struct phs_error **err);

int phs_series_new_i16(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_i32(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_u8(const char *name,
                      const uint8_t *data,
                      uintptr_t len,
                      struct phs_series **out,
                      struct phs_error **err);

int phs_series_new_u16(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_u32(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_u64(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_f32(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_f64(const char *name,
                       const uint8_t *data,
                       uintptr_t len,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_new_text(const char *name,
                        const uint8_t *data,
                        uintptr_t len,
                        struct phs_series **out,
                        struct phs_error **err);

int phs_series_name(const struct phs_series *series,
                    struct phs_bytes **out,
                    struct phs_error **err);

int phs_series_dtype(const struct phs_series *series,
                     struct phs_bytes **out,
                     struct phs_error **err);

int phs_series_len(const struct phs_series *series, uint64_t *out, struct phs_error **err);

int phs_series_null_count(const struct phs_series *series, uint64_t *out, struct phs_error **err);

int phs_series_n_unique(const struct phs_series *series, uint64_t *out, struct phs_error **err);

int phs_series_head(const struct phs_series *series,
                    uint64_t n,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_tail(const struct phs_series *series,
                    uint64_t n,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_slice(const struct phs_series *series,
                     int64_t offset,
                     uint64_t len,
                     struct phs_series **out,
                     struct phs_error **err);

int phs_series_abs(const struct phs_series *series,
                   struct phs_series **out,
                   struct phs_error **err);

int phs_series_round(const struct phs_series *series,
                     uint32_t decimals,
                     int mode,
                     struct phs_series **out,
                     struct phs_error **err);

int phs_series_floor(const struct phs_series *series,
                     struct phs_series **out,
                     struct phs_error **err);

int phs_series_ceil(const struct phs_series *series,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_diff(const struct phs_series *series,
                    int64_t n,
                    int null_behavior,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_interpolate(const struct phs_series *series,
                           int method,
                           struct phs_series **out,
                           struct phs_error **err);

int phs_series_is_null(const struct phs_series *series,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_is_not_null(const struct phs_series *series,
                           struct phs_series **out,
                           struct phs_error **err);

int phs_series_is_nan(const struct phs_series *series,
                      struct phs_series **out,
                      struct phs_error **err);

int phs_series_is_not_nan(const struct phs_series *series,
                          struct phs_series **out,
                          struct phs_error **err);

int phs_series_is_finite(const struct phs_series *series,
                         struct phs_series **out,
                         struct phs_error **err);

int phs_series_is_infinite(const struct phs_series *series,
                           struct phs_series **out,
                           struct phs_error **err);

int phs_series_is_duplicated(const struct phs_series *series,
                             struct phs_series **out,
                             struct phs_error **err);

int phs_series_is_unique(const struct phs_series *series,
                         struct phs_series **out,
                         struct phs_error **err);

int phs_series_is_first_distinct(const struct phs_series *series,
                                 struct phs_series **out,
                                 struct phs_error **err);

int phs_series_is_last_distinct(const struct phs_series *series,
                                struct phs_series **out,
                                struct phs_error **err);

int phs_series_filter(const struct phs_series *series,
                      const struct phs_series *mask,
                      struct phs_series **out,
                      struct phs_error **err);

int phs_series_take(const struct phs_series *series,
                    const uint64_t *indices,
                    uintptr_t len,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_fill_null(const struct phs_series *series,
                         int strategy,
                         bool has_limit,
                         uint64_t limit,
                         struct phs_series **out,
                         struct phs_error **err);

int phs_series_to_frame(const struct phs_series *series,
                        struct phs_dataframe **out,
                        struct phs_error **err);

int phs_series_rename(const struct phs_series *series,
                      const char *name,
                      struct phs_series **out,
                      struct phs_error **err);

int phs_series_cast(const struct phs_series *series,
                    int dtype_code,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_sort(const struct phs_series *series,
                    bool descending,
                    bool nulls_last,
                    bool multithreaded,
                    bool maintain_order,
                    bool has_limit,
                    uint64_t limit,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_unique(const struct phs_series *series,
                      struct phs_series **out,
                      struct phs_error **err);

int phs_series_unique_stable(const struct phs_series *series,
                             struct phs_series **out,
                             struct phs_error **err);

int phs_series_reverse(const struct phs_series *series,
                       struct phs_series **out,
                       struct phs_error **err);

int phs_series_drop_nulls(const struct phs_series *series,
                          struct phs_series **out,
                          struct phs_error **err);

int phs_series_shift(const struct phs_series *series,
                     int64_t periods,
                     struct phs_series **out,
                     struct phs_error **err);

int phs_series_append(const struct phs_series *left,
                      const struct phs_series *right,
                      struct phs_series **out,
                      struct phs_error **err);

int phs_series_binary_op(const struct phs_series *left,
                         const struct phs_series *right,
                         int op,
                         struct phs_series **out,
                         struct phs_error **err);

int phs_series_stat(const struct phs_series *series,
                    int op,
                    uint8_t ddof,
                    bool *has_value_out,
                    double *value_out,
                    struct phs_error **err);

int phs_series_values_bool(const struct phs_series *series,
                           struct phs_bytes **out,
                           struct phs_error **err);

int phs_series_values_i64(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_i8(const struct phs_series *series,
                         struct phs_bytes **out,
                         struct phs_error **err);

int phs_series_values_i16(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_i32(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_u8(const struct phs_series *series,
                         struct phs_bytes **out,
                         struct phs_error **err);

int phs_series_values_u16(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_u32(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_u64(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_f32(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_f64(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_text(const struct phs_series *series,
                           struct phs_bytes **out,
                           struct phs_error **err);

#endif  /* POLARS_HS_H */
