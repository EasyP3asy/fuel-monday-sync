function getCurrentTruckInfo(truckId) {
    return `https://api.keeptruckin.com/api/w3/vehicles/${truckId}/follow?include_groups=true`;
}









module.exports = {    
    getCurrentTruckInfo
};