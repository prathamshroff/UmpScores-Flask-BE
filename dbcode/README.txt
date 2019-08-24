dbcode.py has 3 dependencies
output-data and ref_images folders which can be found on the drive.
name_id.csv which can be found by either asking me (Curtis) or chris

dbcode also utilizes multiple aws resources. 

Dynamodb:
team-stats table which stores all of the data in output-data/team-stats
game-stats table which stores all of the data in output-data/game-stats
umpire-id lookup table which stores name_id.csv. Used as a hash between uuid and umpire name

CloudSearch:
umpires cloudsearch which serves as an auto complete for umpire names which can later be keyed
	against a dynamodb table

S3:
refrating bucket which holds all of our media such as umpire profile images