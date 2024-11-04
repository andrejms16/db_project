--CREATE USER dbproject WITH ENCRYPTED PASSWORD 'admin';

CREATE SCHEMA IF NOT EXISTS fires AUTHORIZATION dbproject;

-- Creating table for FireStation
CREATE TABLE fires.fire_station (
    id SERIAL PRIMARY KEY,
    address VARCHAR(256) NOT NULL
);

-- Creating table for Firefighter
CREATE TABLE fires.firefighter (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    firestation_id INT NOT NULL,
    CONSTRAINT firefighter_name_unique UNIQUE (name),
    FOREIGN KEY (firestation_id) REFERENCES fires.fire_station(id)
);

-- Creating table for Vehicle
CREATE TABLE fires.vehicle (
    id SERIAL PRIMARY KEY,
    car_registration VARCHAR(10) NOT NULL,
    model VARCHAR(256) NOT NULL,
    maker VARCHAR(256) NOT NULL,
    firestation_id INT NOT NULL,
    CONSTRAINT car_registration_unique UNIQUE (car_registration),
    FOREIGN KEY (firestation_id) REFERENCES fires.fire_station(id)
);

-- Creating table for CanadianFireIndex
CREATE TABLE fires.canadian_fire_index (
    id SERIAL PRIMARY KEY,
	acronym VARCHAR(4) NOT NULL,
    name VARCHAR(256) NOT NULL,
    CONSTRAINT canadian_fire_index_acronym_unique UNIQUE (acronym)
);

-- Creating table for AlertSource
CREATE TABLE fires.alert_source (
    id SERIAL PRIMARY KEY,
    description VARCHAR(256) NOT NULL,
    CONSTRAINT alert_source_description_unique UNIQUE (description)
);

-- Creating table for CauseGroup
CREATE TABLE fires.cause_group (
    id SERIAL PRIMARY KEY,
    description VARCHAR(256) NOT NULL,
    CONSTRAINT cause_group_description_unique UNIQUE (description)
);

-- Creating table for CauseType
CREATE TABLE fires.cause_type (
    id SERIAL PRIMARY KEY,
    description VARCHAR(256) NOT NULL,
    --cause_group_id INT NOT NULL,
    FOREIGN KEY (cause_group_id) REFERENCES fires.cause_group(id),
    CONSTRAINT cause_type_description_unique UNIQUE (description)
);

-- Creating table for cause
CREATE TABLE fires.cause (
    codcausa INT PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
	cause_type_id INT,
	cause_group_id INT,
	FOREIGN KEY (cause_type_id) REFERENCES fires.cause_type(id),
	FOREIGN KEY (cause_group_id) REFERENCES fires.cause_group(id),
    CONSTRAINT cause_name_unique UNIQUE (name)
);

-- Creating table for AreaType
CREATE TABLE fires.area_type (
    id SERIAL PRIMARY KEY,
    description TEXT,
    CONSTRAINT area_type_description_unique UNIQUE (description)
);

-- Creating table for District
CREATE TABLE fires.district (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL    
);

-- Creating table for Municipality
CREATE TABLE fires.municipality (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    district_id INT NOT NULL,
    FOREIGN KEY (district_id) REFERENCES fires.district(id)    
);

-- Creating table for Neighborhood
CREATE TABLE fires.neighborhood (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    municipality_id INT NOT NULL,
    freguesia_INE INT,
    FOREIGN KEY (municipality_id) REFERENCES fires.municipality(id)    
);

-- Creating table for RNMPF
CREATE TABLE fires.RNMPF (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    CONSTRAINT RNMPF_name_unique UNIQUE (name)
);

-- Creating table for RNAP
CREATE TABLE fires.RNAP (
    id SERIAL PRIMARY KEY,
    name VARCHAR(256) NOT NULL,
    CONSTRAINT RNAP_name_unique UNIQUE (name)
);

-- Creating table for Fire
CREATE TABLE fires.fire (
	id SERIAL PRIMARY KEY,
    id_SGIF VARCHAR NOT NULL,
    id_ANEPC INT NOT NULL,
    year_number INT NOT NULL,
    month_number INT NOT NULL,
    day_number INT NOT NULL,
    alert_time TIMESTAMP NOT NULL,
    first_intervention TIMESTAMP,
    extinction TIMESTAMP,
    --duration DECIMAL,
    address VARCHAR(256),
    x_militar_position DECIMAL(25, 18),
    y_militar_position DECIMAL(25, 18),
    latitude DECIMAL(25, 18) NOT NULL,
    longitude DECIMAL(25, 18) NOT NULL,
    x_etrs89 DECIMAL(25, 18),
    y_etrs89 DECIMAL(25, 18),
    alert_source_id INT NOT NULL,
    cause_id INT NOT NULL,
    neighborhood_id INT,
	rnmpf_id INT,
	rnap_id INT,	
    FOREIGN KEY (rnap_id) REFERENCES fires.rnap(id),
    FOREIGN KEY (rnmpf_id) REFERENCES fires.rnmpf(id),
    FOREIGN KEY (alert_source_id) REFERENCES fires.alert_source(id),
    FOREIGN KEY (cause_id) REFERENCES fires.cause(codcausa),
    FOREIGN KEY (neighborhood_id) REFERENCES fires.neighborhood(id),	
	CONSTRAINT fire_id_SGIF_unique UNIQUE (id)
);

-- Creating table for BurnedArea
CREATE TABLE fires.burned_area (
    id SERIAL PRIMARY KEY,
    burned_area DECIMAL(25, 15),
    area_type_id INT NOT NULL,
    fire_id INT NOT NULL,
    FOREIGN KEY (area_type_id) REFERENCES fires.area_type(id),
    FOREIGN KEY (fire_id) REFERENCES fires.Fire(id)
);

-- Creating table for FireRiskIndex
CREATE TABLE fires.fire_risk_index (
    fire_id INT,
	canadian_fire_index_id INT,
	index_value DECIMAL(25,18), 
	PRIMARY KEY (fire_id , canadian_fire_index_id ),
	FOREIGN KEY (canadian_fire_index_id) REFERENCES fires.canadian_fire_index(id),
	FOREIGN KEY (fire_id) REFERENCES fires.fire(id)
);

-- Creating table for Vehicles used in the fire
CREATE TABLE fires.fire_vehicle (
    vehicle_id INT NOT NULL,
	fire_id INT NOT NULL,
	PRIMARY KEY (vehicle_id , fire_id ),
  	FOREIGN KEY (vehicle_id) REFERENCES fires.vehicle(id),
	FOREIGN KEY (fire_id) REFERENCES fires.fire(id)
);

-- Creating table for Firefighters worked in the fire
CREATE TABLE fires.fire_firefighter (
    firefighter_id INT NOT NULL,
	fire_id INT NOT NULL,
	PRIMARY KEY (firefighter_id , fire_id ),
  	FOREIGN KEY (firefighter_id) REFERENCES fires.firefighter(id),
	FOREIGN KEY (fire_id) REFERENCES fires.fire(id)
);
