// queries.js


function getAllItemsInBoard(boardID) {
    return `
        query {
            boards(ids: ${boardID}) {
                items_page(limit: 200) {
                    cursor
                    items {
                        id
                        name
                    }
                }
            }
        }
    `;
}






function deleteGoupQuery(boardID, groupID) {
    return `
        mutation {
            delete_group(board_id: ${boardID}, group_id: "${groupID}") {
                id
                deleted
            }
        }
    `;
}

function duplicateGroupQuery(boardID, groupID) {
    return `
        mutation {
            duplicate_group(board_id: ${boardID}, group_id: "${groupID}", add_to_top: true) {
                id
            }
        }
    `;
}

function updateGroupTitleQuery(boardID, groupID, newTitle) {
    return `
        mutation {
            update_group(board_id: ${boardID}, group_id: "${groupID}", group_attribute: title, new_value: "${newTitle}") {
                id
            }
        }
    `;
}


function updateGroupColorQuery(boardID,groupID,color){
    return `
        mutation { 
            update_group (board_id: ${boardID}, group_id: "${groupID}", group_attribute: color, new_value: "${color}") {
               id 
            } 
        }
    `;
}



function updateMultipleColumnValuesQuery(boardID, rowID, columnValues) {
    return `
        mutation {
            change_multiple_column_values(
                board_id: ${boardID},
                item_id: ${rowID},
                column_values: "${gqlEscape(JSON.stringify(columnValues))}"
            ) {
                id
            }
        }
    `;
}



function updateMultipleAlliasColumnValuesQuery(boardID, rowID, columnValues) {
    return `
        item_${rowID} : change_multiple_column_values(
            board_id: ${boardID},
            item_id: ${rowID},
            column_values: "${gqlEscape(JSON.stringify(columnValues))}"
        ){
            id
        }        
    `;
}


function createMultipleAlliasColumnValuesQuery(index,boardID, groupId,itemName, columnValues) {
    return `
        item_${index} : create_item(
            board_id: ${boardID},
            group_id: "${groupId}",
            item_name: "${itemName}",
            column_values: "${gqlEscape(JSON.stringify(columnValues))}"
        ){
            id
        }        
    `;
}





function updateSimpleColumnValueQuery(boardID,rowID,itemName){
    return `
        mutation { 
            change_simple_column_value (board_id: ${boardID}, item_id: ${rowID}, column_id: "name", value: "${itemName.toUpperCase()}") { 
                id  
            }
        }
    `;
}


function changeGroupPositionQuery(boardID,groupId,relativeTo_GroupID){
    return `
        mutation {
                update_group (board_id: ${boardID}, group_id: "${groupId}", group_attribute: relative_position_before, new_value: "${relativeTo_GroupID}") { 
                    id
                } 
        }
    `;    
}


function getGroupsIDandTitleQuery(boardID){

    return  `
        query {  
            boards (ids: ${boardID}) {
                   groups {   
                        title  
                        id  
                    }  
            }  
        }
    `;
}






function getColumnValues(boardID){
    return `
        query {
            boards(ids: ${boardID}) {
                items_page {  
                    items { 
                        id  
                        name  
                        group {  
                            id  
                        }  
                        column_values {  
                            id 
                            text 
                            value 
                        } 
                    } 
                } 
            } 
        }
    `;
}


// Maximum number is 500 items per page

function getAllRowIDs(boardID){
    return `
        query {
            boards (ids: ${boardID}){   
                items_page (limit: 499){
                    cursor
                    items {
                        id
                        name
                    }
                }
            }
        }
    `;
}


// get connected boardID and rowId

function getConnectedBoardId_ItemId(itemId,CONNECTED_BOARD_COLUMN_ID){
    return `
        query{
            items(ids: [${itemId}]){
                column_values (ids:["${CONNECTED_BOARD_COLUMN_ID}"]){
                    ... on BoardRelationValue {
                        linked_items {
                            id
                            name
                            board { 
                                id 
                            }
                        }
                        
                    }
                }
            }
        }
  `;
}


function getColumnValuesFilterGroupAndColumn(boardID,groupId,columnId,limit=200){
    return `
        query {
            boards(ids: ${boardID}) {
                items_page( 
                    limit: ${limit}
                    query_params: {
                        rules: [                       
                        {
                            column_id: "group"           
                            compare_value: ["${groupId}"]  
                            operator: any_of
                        }
                        ]
                       
                    }  
                
                ) {
                    cursor
                    items {
                        id
                        column_values(ids: "${columnId}") {          
                            text                            
                        }
                    }
                }
            }
        }
    `;
}

function gqlEscape(s="") {
  return String(s)
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r");
}




module.exports = {
    createMultipleAlliasColumnValuesQuery,
    deleteGoupQuery,
    duplicateGroupQuery,
    getAllItemsInBoard,
    getColumnValuesFilterGroupAndColumn,
    updateGroupTitleQuery,
    updateGroupColorQuery,
    updateMultipleColumnValuesQuery,
    updateMultipleAlliasColumnValuesQuery,
    updateSimpleColumnValueQuery,
    changeGroupPositionQuery,
    getGroupsIDandTitleQuery,    
    getColumnValues,
    getConnectedBoardId_ItemId,
    getAllRowIDs
};
