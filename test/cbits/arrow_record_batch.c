#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};

struct SchemaPrivate {
    char *format;
    char *name;
    struct ArrowSchema **children;
    int64_t n_children;
    struct ArrowSchema *self;
};

struct ArrayPrivate {
    const void **buffers;
    void **owned_buffers;
    int64_t n_owned_buffers;
    struct ArrowArray **children;
    int64_t n_children;
    struct ArrowArray *self;
};

static char *copy_string(const char *value) {
    size_t len = strlen(value) + 1;
    char *out = (char *)malloc(len);
    if (out != NULL) {
        memcpy(out, value, len);
    }
    return out;
}

static void release_schema(struct ArrowSchema *schema) {
    if (schema == NULL || schema->release == NULL) {
        return;
    }
    struct SchemaPrivate *private_data = (struct SchemaPrivate *)schema->private_data;
    schema->release = NULL;
    if (private_data == NULL) {
        return;
    }
    for (int64_t i = 0; i < private_data->n_children; i++) {
        struct ArrowSchema *child = private_data->children[i];
        if (child != NULL && child->release != NULL) {
            child->release(child);
        }
    }
    free(private_data->children);
    free(private_data->format);
    free(private_data->name);
    if (private_data->self != schema) {
        private_data->self->release = NULL;
    }
    free(private_data->self);
    free(private_data);
}

static void release_array(struct ArrowArray *array) {
    if (array == NULL || array->release == NULL) {
        return;
    }
    struct ArrayPrivate *private_data = (struct ArrayPrivate *)array->private_data;
    array->release = NULL;
    if (private_data == NULL) {
        return;
    }
    for (int64_t i = 0; i < private_data->n_children; i++) {
        struct ArrowArray *child = private_data->children[i];
        if (child != NULL && child->release != NULL) {
            child->release(child);
        }
    }
    for (int64_t i = 0; i < private_data->n_owned_buffers; i++) {
        free(private_data->owned_buffers[i]);
    }
    free(private_data->owned_buffers);
    free(private_data->children);
    free((void *)private_data->buffers);
    if (private_data->self != array) {
        private_data->self->release = NULL;
    }
    free(private_data->self);
    free(private_data);
}

static struct ArrowSchema *make_schema(const char *format, const char *name, int nullable, int64_t n_children, struct ArrowSchema **children) {
    struct ArrowSchema *schema = (struct ArrowSchema *)calloc(1, sizeof(struct ArrowSchema));
    struct SchemaPrivate *private_data = (struct SchemaPrivate *)calloc(1, sizeof(struct SchemaPrivate));
    if (schema == NULL || private_data == NULL) {
        free(schema);
        free(private_data);
        return NULL;
    }
    private_data->format = copy_string(format);
    private_data->name = copy_string(name);
    private_data->children = children;
    private_data->n_children = n_children;
    private_data->self = schema;
    if (private_data->format == NULL || private_data->name == NULL) {
        free(private_data->format);
        free(private_data->name);
        free(private_data);
        free(schema);
        return NULL;
    }
    schema->format = private_data->format;
    schema->name = private_data->name;
    schema->metadata = NULL;
    schema->flags = nullable ? 2 : 0;
    schema->n_children = n_children;
    schema->children = children;
    schema->dictionary = NULL;
    schema->release = release_schema;
    schema->private_data = private_data;
    return schema;
}

static struct ArrowArray *make_array(int64_t length, int64_t null_count, int64_t n_buffers, const void **buffers, int64_t n_owned_buffers, void **owned_buffers, int64_t n_children, struct ArrowArray **children) {
    struct ArrowArray *array = (struct ArrowArray *)calloc(1, sizeof(struct ArrowArray));
    struct ArrayPrivate *private_data = (struct ArrayPrivate *)calloc(1, sizeof(struct ArrayPrivate));
    if (array == NULL || private_data == NULL) {
        free(array);
        free(private_data);
        return NULL;
    }
    private_data->buffers = buffers;
    private_data->owned_buffers = owned_buffers;
    private_data->n_owned_buffers = n_owned_buffers;
    private_data->children = children;
    private_data->n_children = n_children;
    private_data->self = array;
    array->length = length;
    array->null_count = null_count;
    array->offset = 0;
    array->n_buffers = n_buffers;
    array->n_children = n_children;
    array->buffers = buffers;
    array->children = children;
    array->dictionary = NULL;
    array->release = release_array;
    array->private_data = private_data;
    return array;
}

static struct ArrowArray *make_utf8_name_array(void) {
    uint8_t *validity = (uint8_t *)malloc(1);
    int32_t *offsets = (int32_t *)malloc(4 * sizeof(int32_t));
    uint8_t *values = (uint8_t *)malloc(8);
    const void **buffers = (const void **)calloc(3, sizeof(void *));
    void **owned_buffers = (void **)calloc(3, sizeof(void *));
    if (validity == NULL || offsets == NULL || values == NULL || buffers == NULL || owned_buffers == NULL) {
        free(validity);
        free(offsets);
        free(values);
        free(buffers);
        free(owned_buffers);
        return NULL;
    }
    validity[0] = 0x03;
    offsets[0] = 0;
    offsets[1] = 5;
    offsets[2] = 8;
    offsets[3] = 8;
    memcpy(values, "AliceBob", 8);
    buffers[0] = validity;
    buffers[1] = offsets;
    buffers[2] = values;
    owned_buffers[0] = validity;
    owned_buffers[1] = offsets;
    owned_buffers[2] = values;
    return make_array(3, 1, 3, buffers, 3, owned_buffers, 0, NULL);
}

static struct ArrowArray *make_int64_age_array(void) {
    uint8_t *validity = (uint8_t *)malloc(1);
    int64_t *values = (int64_t *)malloc(3 * sizeof(int64_t));
    const void **buffers = (const void **)calloc(2, sizeof(void *));
    void **owned_buffers = (void **)calloc(2, sizeof(void *));
    if (validity == NULL || values == NULL || buffers == NULL || owned_buffers == NULL) {
        free(validity);
        free(values);
        free(buffers);
        free(owned_buffers);
        return NULL;
    }
    validity[0] = 0x05;
    values[0] = 34;
    values[1] = 0;
    values[2] = 29;
    buffers[0] = validity;
    buffers[1] = values;
    owned_buffers[0] = validity;
    owned_buffers[1] = values;
    return make_array(3, 1, 2, buffers, 2, owned_buffers, 0, NULL);
}

int phs_test_people_record_batch(void **schema_out, void **array_out) {
    if (schema_out == NULL || array_out == NULL) {
        return -1;
    }
    *schema_out = NULL;
    *array_out = NULL;

    struct ArrowSchema *name_schema = make_schema("u", "name", 1, 0, NULL);
    struct ArrowSchema *age_schema = make_schema("l", "age", 1, 0, NULL);
    struct ArrowSchema **schema_children = (struct ArrowSchema **)calloc(2, sizeof(struct ArrowSchema *));
    struct ArrowArray *name_array = make_utf8_name_array();
    struct ArrowArray *age_array = make_int64_age_array();
    struct ArrowArray **array_children = (struct ArrowArray **)calloc(2, sizeof(struct ArrowArray *));
    const void **top_buffers = (const void **)calloc(1, sizeof(void *));

    if (name_schema == NULL || age_schema == NULL || schema_children == NULL || name_array == NULL || age_array == NULL || array_children == NULL || top_buffers == NULL) {
        if (name_schema != NULL && name_schema->release != NULL) name_schema->release(name_schema);
        if (age_schema != NULL && age_schema->release != NULL) age_schema->release(age_schema);
        if (name_array != NULL && name_array->release != NULL) name_array->release(name_array);
        if (age_array != NULL && age_array->release != NULL) age_array->release(age_array);
        free(schema_children);
        free(array_children);
        free((void *)top_buffers);
        return -1;
    }

    schema_children[0] = name_schema;
    schema_children[1] = age_schema;
    array_children[0] = name_array;
    array_children[1] = age_array;
    top_buffers[0] = NULL;

    struct ArrowSchema *schema = make_schema("+s", "", 0, 2, schema_children);
    struct ArrowArray *array = make_array(3, 0, 1, top_buffers, 0, NULL, 2, array_children);
    if (schema == NULL || array == NULL) {
        if (schema != NULL && schema->release != NULL) schema->release(schema);
        if (array != NULL && array->release != NULL) array->release(array);
        return -1;
    }

    *schema_out = schema;
    *array_out = array;
    return 0;
}

int phs_test_age_array(void **schema_out, void **array_out) {
    if (schema_out == NULL || array_out == NULL) {
        return -1;
    }
    *schema_out = NULL;
    *array_out = NULL;

    struct ArrowSchema *schema = make_schema("l", "age", 1, 0, NULL);
    struct ArrowArray *array = make_int64_age_array();
    if (schema == NULL || array == NULL) {
        if (schema != NULL && schema->release != NULL) schema->release(schema);
        if (array != NULL && array->release != NULL) array->release(array);
        return -1;
    }

    *schema_out = schema;
    *array_out = array;
    return 0;
}
