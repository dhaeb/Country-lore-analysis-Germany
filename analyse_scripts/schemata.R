klSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("MESS_DATUM", "date"),
  structField("QUALITAETS_NIVEAU", "double"),
  structField("LUFTTEMPERATUR", "double"),
  structField("DAMPFDRUCK", "double"),
  structField("BEDECKUNGSGRAD", "double"),
  structField("LUFTDRUCK_STATIONSHOEHE", "double"),
  structField("REL_FEUCHTE", "double"),
  structField("WINDGESCHWINDIGKEIT", "double"),
  structField("LUFTTEMPERATUR_MAXIMUM", "double"),
  structField("LUFTTEMPERATUR_MINIMUM", "double"),
  structField("LUFTTEMP_AM_ERDB_MINIMUM", "double"),
  structField("WINDSPITZE_MAXIMUM", "double"),
  structField("NIEDERSCHLAGSHOEHE", "double"),
  structField("NIEDERSCHLAGSHOEHE_IND", "double"),
  structField("SONNENSCHEINDAUER", "double"),
  structField("SCHNEEHOEHE", "double")
)

metaSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("von_datum", "string"),
  structField("bis_datum", "string"),
  structField("Statationshoehe", "double"),
  structField("longitude", "double"),
  structField("latitude", "double"),
  structField("Stationsname", "string"),
  structField("Bundesland", "string"),
  structField("Lage", "string")
)

morePrecipSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("MESS_DATUM", "date"),
  structField("QUALITAETS_NIVEAU", "double"),
  structField("NIEDERSCHLAGSHOEHE","double"),
  structField("NIEDERSCHLAGSHOEHE_IND","double"),
  structField("SCHNEEHOEHE","double")
)

solarSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("MESS_DATUM", "date"),
  structField("QUALITAETS_NIVEAU", "double"),
  structField("SONNENSCHEINDAUER","double"),
  structField("GLOBAL_KW_J", "double"),
  structField("DIFFUS_HIMMEL_KW_J", "double"),
  structField("ATMOSPHAERE_LW_J", "double")
)

soilSchema <- structType(
  structField("STATIONS_ID", "integer"),
  structField("MESS_DATUM", "date"),
  structField("QUALITAETS_NIVEAU", "double"),
  structField("ERDBODENTEMPERATUR", "double"),
  structField("MESS_TIEFE", "double"),
  structField("ERDBODENTEMPERATUR", "double"),
  structField("MESS_TIEFE", "double"),
  structField("ERDBODENTEMPERATUR", "double"),
  structField("MESS_TIEFE", "double"),
  structField("ERDBODENTEMPERATUR", "double"),
  structField("MESS_TIEFE", "double"),
  structField("ERDBODENTEMPERATUR", "double"),
  structField("MESS_TIEFE", "double")
)

