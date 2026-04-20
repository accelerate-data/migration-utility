CREATE TABLE shared.opportunity_cases (
  opportunity_id integer NOT NULL,
  case_id integer NOT NULL,
  owner_team varchar(100) NOT NULL,
  status varchar(50) NOT NULL
);
