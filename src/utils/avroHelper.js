const flattenAvro = (obj) => {
  if (obj === null || typeof obj !== "object") return obj;

  const keys = Object.keys(obj);
  // Check if it's an Avro union wrapper
  if (
    keys.length === 1 &&
    ["string", "long", "int", "boolean", "double", "float"].includes(keys[0])
  ) {
    return flattenAvro(obj[keys[0]]);
  }

  const flattened = {};
  for (const [key, value] of Object.entries(obj)) {
    flattened[key] = flattenAvro(value);
  }
  return flattened;
};

export { flattenAvro };
