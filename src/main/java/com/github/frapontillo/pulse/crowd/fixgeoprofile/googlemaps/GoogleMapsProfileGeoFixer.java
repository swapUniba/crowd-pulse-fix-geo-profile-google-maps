package com.github.frapontillo.pulse.crowd.fixgeoprofile.googlemaps;

import com.github.frapontillo.pulse.crowd.data.entity.Profile;
import com.github.frapontillo.pulse.crowd.data.plugin.ConnectionFetcher;
import com.github.frapontillo.pulse.crowd.fixgeoprofile.IProfileGeoFixerOperator;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.spi.IPluginConfig;
import com.github.frapontillo.pulse.spi.PluginConfigHelper;
import com.github.frapontillo.pulse.spi.VoidConfig;
import com.github.frapontillo.pulse.util.PulseLogger;
import com.github.frapontillo.pulse.util.StringUtil;
import com.google.gson.JsonElement;
import com.google.maps.GeoApiContext;
import com.google.maps.GeocodingApi;
import com.google.maps.errors.OverDailyLimitException;
import com.google.maps.errors.OverQueryLimitException;
import com.google.maps.model.GeocodingResult;
import org.apache.logging.log4j.Logger;
import rx.Observable;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of an {@link IPlugin} that accepts and streams {@link Profile}s after attempting
 * a geo-location fix on them, using the Google Maps reverse geocoding APIs.
 * <p/>
 * To use this plugin, create a {@code geocoding.properties} file in the classpath with a {@code
 * geocoding.apiKey} property set to your Google Maps API key.
 *
 * @author Francesco Pontillo
 */
public class GoogleMapsProfileGeoFixer extends IPlugin<Profile, Profile, GoogleMapsProfileGeoFixer.GoogleMapsProfileGeoFixerConfig> {
    public final static String PLUGIN_NAME = "googlemaps";
    private final static String PROP_GEOCODING_APIKEY = "geocoding.apiKey";

    private static final Logger logger = PulseLogger.getLogger(GoogleMapsProfileGeoFixer.class);

    private static String[] API_KEYS;
    private static byte serviceNumber = 0;

    private GeoApiContext context;

    public GoogleMapsProfileGeoFixer() {
        context = new GeoApiContext().setApiKey(readApiKey())
                // if the request doesn't succeed within 10 seconds, discard it
                .setRetryTimeout(10, TimeUnit.SECONDS);
    }

    @Override public String getName() {
        return PLUGIN_NAME;
    }

    @Override public GoogleMapsProfileGeoFixerConfig getNewParameter() {
        return new GoogleMapsProfileGeoFixerConfig();
    }

    @Override protected Observable.Operator<Profile, Profile> getOperator(GoogleMapsProfileGeoFixerConfig parameters) {
        return new IProfileGeoFixerOperator(this) {
            @Override public Double[] getCoordinates(Profile profile) {
                if (StringUtil.isNullOrEmpty(profile.getLocation())) {
                    return null;
                }

                // the profile already has the lat and lng data
                if (profile.getLatitude() != null && profile.getLongitude() != null) {
                    return new Double[]{profile.getLatitude(), profile.getLongitude()};
                }

                GeocodingResult[] results = null;
                Double[] coordinates = null;

                // attempt a forward geocoding (from address to lat-lng)
                try {
                    results =
                            GeocodingApi.newRequest(context).address(profile.getLocation()).await();
                } catch (OverDailyLimitException | OverQueryLimitException ex) {

                    //check remaining API KEYS available
                    if (serviceNumber < API_KEYS.length) {
                        serviceNumber++;
                    } else {
                        serviceNumber = 0;
                    }
                    logger.info("Switching API KEY to " + API_KEYS[serviceNumber]);
                    context = new GeoApiContext().setApiKey(API_KEYS[serviceNumber]);

                    // retry request
                    try {
                        results = GeocodingApi.newRequest(context).address(profile.getLocation()).await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

                // edit and notify the profile only if lat-lng coordinates were found
                if (results != null && results.length > 0) {
                    coordinates = new Double[]{results[0].geometry.location.lat,
                            results[0].geometry.location.lng};

                } else if (parameters != null && parameters.isSetCoordinateNull()) {
                    coordinates = new Double[]{-1.0, -1.0};
                }

                return coordinates;
            }
        };
    }

    /**
     * Read the API key from the {@code geocoding.properties} file in the {@code geocoding.apiKey}
     * property.
     *
     * @return The Google Maps API key.
     */
    private String readApiKey() {
        InputStream configInput =
                getClass().getClassLoader().getResourceAsStream("geocoding.properties");
        Properties prop = new Properties();
        try {
            prop.load(configInput);
            String keys = prop.getProperty(PROP_GEOCODING_APIKEY);
            API_KEYS = keys.split(",");
            return API_KEYS[0];

        } catch (Exception exception) {
            PulseLogger.getLogger(GoogleMapsProfileGeoFixer.class)
                    .error("Error while loading Google Maps configuration", exception);
            return "";
        }
    }

    public class GoogleMapsProfileGeoFixerConfig implements IPluginConfig<GoogleMapsProfileGeoFixer.GoogleMapsProfileGeoFixerConfig> {

        /**
         * If true, the plug-in sets to -1 coordinates that the Google Maps service does not find the coordinates for
         * a specified text-location.
         */
        private boolean setCoordinateToNull;

        public boolean isSetCoordinateNull() {
            return setCoordinateToNull;
        }

        public void setSetCoordinateNull(boolean setCoordinateNull) {
            this.setCoordinateToNull = setCoordinateNull;
        }

        @Override
        public GoogleMapsProfileGeoFixer.GoogleMapsProfileGeoFixerConfig buildFromJsonElement(JsonElement json) {
            return PluginConfigHelper.buildFromJson(json, GoogleMapsProfileGeoFixer.GoogleMapsProfileGeoFixerConfig.class);
        }

    }

}
