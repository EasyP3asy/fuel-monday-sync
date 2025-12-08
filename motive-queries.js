function getCurrentTruckInfo(truckId) {
    return `https://api.gomotive.com/api/w3/vehicles/${truckId}/follow?include_groups=true`;
}









module.exports = {    
    getCurrentTruckInfo
};