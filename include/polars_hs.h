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

int phs_read_parquet(const char *path, struct phs_dataframe **out, struct phs_error **err);

int phs_dataframe_new(const struct phs_series *const *series,
                      uintptr_t len,
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

int phs_scan_parquet(const char *path, struct phs_lazyframe **out, struct phs_error **err);

int phs_lazyframe_collect(const struct phs_lazyframe *lazyframe,
                          struct phs_dataframe **out,
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

int phs_series_head(const struct phs_series *series,
                    uint64_t n,
                    struct phs_series **out,
                    struct phs_error **err);

int phs_series_tail(const struct phs_series *series,
                    uint64_t n,
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

int phs_series_values_bool(const struct phs_series *series,
                           struct phs_bytes **out,
                           struct phs_error **err);

int phs_series_values_i64(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_f64(const struct phs_series *series,
                          struct phs_bytes **out,
                          struct phs_error **err);

int phs_series_values_text(const struct phs_series *series,
                           struct phs_bytes **out,
                           struct phs_error **err);

#endif  /* POLARS_HS_H */
