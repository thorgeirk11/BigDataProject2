drop table if exists einstaklingar;
CREATE TABLE einstaklingar
(
id serial NOT NULL,
EinstID int,
Nafn character varying(125),
Fdagur timestamp not null,
Kyn character varying(5),
FelagISI character varying(125),
Netfang character varying(125),
Heimilisfang1 character varying(125),
Heimilisfang2 character varying(125),
Heimilisfang3 character varying(125),
Simi1 character varying(125),
Simi2 character varying(125),
Simi3 character varying(125),
Timastimpill timestamp,
Haed character varying(125),
realID int
);
DROP TRIGGER IF EXISTS CheckPerson on Einstaklingar;
Drop function if exists CheckPerson();
CREATE OR REPLACE FUNCTION CheckPerson()
RETURNS TRIGGER
AS
$$
BEGIN
NEW.realID := 0;
	IF NOT EXISTS( select * from einstaklingar where nafn = NEW.nafn and fdagur = new.fdagur) THEN
		NEW.realID := new.id;
	ELSE
		NEW.realID := (select min(id) from einstaklingar where nafn = new.nafn and fdagur = new.fdagur);
	END IF;
	return NEW;
END
$$
LANGUAGE plpgsql;

CREATE TRIGGER CheckPerson
BEFORE INSERT
ON einstaklingar FOR EACH ROW EXECUTE PROCEDURE CheckPerson();

COPY einstaklingar(EinstID,Nafn, Fdagur, Kyn, FelagISI,Netfang, Heimilisfang1,Heimilisfang2,Heimilisfang3,Simi1,Simi2,Simi3, Timastimpill,Haed)
FROM '/home/gag1/blak-einstaklingar.csv' DELIMITER ';' CSV HEADER;

--drop table einstaklingar

select id,nafn,realID from einstaklingar;


COPY einstaklingar to '/home/gag1/nytt/einstaklingar-nytt.csv' DELIMITER ';' CSV HEADER;