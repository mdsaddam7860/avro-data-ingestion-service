function modifyData(jsonData, heapTableName2 = null) {
  let modifiedData = null;

  if (heapTableName2 === "users" && jsonData.length > 0) {
    const payload1 = jsonData
      .filter((item) => item && item?.company_sfdcid && item?.company_name)
      .map((item) => ({
        name: item.company_name,
        sourceId: item.company_sfdcid,
      }));

    const payload2 = jsonData
      .filter(
        (item) =>
          item &&
          item?.user_email &&
          item?.company_name &&
          item?.company_sfdcid &&
          item?.user_id
      )
      .map((item) => {
        // 1. Extract first Name and last Name
        const { firstName, lastName } = extractNames(item?.user_full_name);

        // 2. Explicitly return the new object
        return {
          firstName,
          lastName,
          companyId: `srcid-${item.company_sfdcid}`,
          email: item.user_email,
          // position: item.user_email,
          externalId: String(item.user_id),
        };
      });

    const data = {
      Results1: payload1,
      Results2: payload2,
    };
    return data;
  } else {
    // Modified function - keeps ONLY yellow fields + table_name
    // modifiedData = jsonData.map((item) => {
    //   // Create new object with only yellow fields
    //   const modifiedItem = {};
    //   // Copy only yellow-highlighted fields from original item
    //   yellowFields.forEach((field) => {
    //     if (item.hasOwnProperty(field)) {
    //       modifiedItem[field] = item[field];
    //     }
    //   });
    //   // Add the table_name field
    //   modifiedItem.table_name = heapTableName2;
    //   return modifiedItem;
    // });

    modifiedData = jsonData
      .filter((item) => {
        return item && item?.user_id;
      })
      .map((item) => ({
        externalId: item?.user_id,
        action: heapTableName2,
      }));
  }
  return modifiedData;
}

export { modifyData };
