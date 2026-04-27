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

typedef struct phs_bytes {
  uint8_t _private[0];
} phs_bytes;

typedef struct phs_error {
  uint8_t _private[0];
} phs_error;

typedef struct phs_dataframe {
  uint8_t _private[0];
} phs_dataframe;

typedef struct phs_lazyframe {
  uint8_t _private[0];
} phs_lazyframe;

typedef struct phs_expr {
  uint8_t _private[0];
} phs_expr;

uint32_t phs_version_major(void);

uint32_t phs_version_minor(void);

uintptr_t phs_bytes_len(const struct phs_bytes *ptr);

const unsigned char *phs_bytes_data(const struct phs_bytes *ptr);

void phs_bytes_free(struct phs_bytes *ptr);

int phs_error_code(const struct phs_error *error);

const char *phs_error_message(const struct phs_error *error);

void phs_error_free(struct phs_error *error);

void phs_dataframe_free(struct phs_dataframe *ptr);

void phs_lazyframe_free(struct phs_lazyframe *ptr);

void phs_expr_free(struct phs_expr *ptr);

#endif  /* POLARS_HS_H */
