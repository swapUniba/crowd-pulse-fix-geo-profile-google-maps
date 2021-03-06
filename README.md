crowd-pulse-fix-geo-profile-google-maps
=======================================

Google Maps based Crowd Pulse profile geo-location fixer.

---------------------------------------

To fix geolocation for profiles you need to create a `geocoding.properties` file and
put it into the resources directory (must be accessible by the class loader).

This file must contain the `geocoding.apiKey` property, whose value (or values, comma separated) must be a 
Google Geocoding API key(s). To get your key, do the following:

1. If you don't have on already, create a new project on the [Google Developers Console]
(https://console.developers.google.com).
2. Go to "APIs & Auth".
3. Go to the "Credentials" sub-section and create a new Server Key. That's your key.
4. Go to the "APIs" sub-section and enable the "Geocoding API" from the Google Maps service.

---------------------------------------

Example of usage:
```json
"profile-fix-position": {
      "plugin": "googlemaps",
      "config": {
        "setCoordinateToNull": "true"
      }
    },
```