CREATE INDEX segments_start_geo ON segments
    USING GIST (CAST(ST_MakePoint(start_lng, start_lat) AS geography));
