# MD-RP-FRPP-Validation-Tool
Data validation tools for data submitted for the Federal Real Property Profile. Tests addresses to determine if they can be geocoded by ArcGIS REST API and also for distance variance from reported latitude/longitude. 

## Getting Started


### Getting the Data
Data is housed in MD SQL Server. The user must complete information in the config file with access information. 

### Geocoding Address
Data from the FRPP Database is submitted to the REST API with inputs being Address, City, Region (State) and Postal (Zip Code). The JSON response from the API is captured. A summary of the service can be found [here.](https://developers.arcgis.com/rest/geocode/api-reference/geocoding-geocode-addresses.htm) A summary of the output can be found [here.](https://developers.arcgis.com/rest/geocode/api-reference/geocoding-service-output.htm)

## License

This project is licensed under the Creative Commons Zero v1.0 Universal License - see the [LICENSE.md](https://github.com/GSA/wb2/blob/master/LICENSE) file for details
