CREATE VIEW silver.vw_CrossDbProfile
AS
SELECT
    CrossDbProfileKey,
    EmployeeAlternateKey,
    EmployeeTitle
FROM silver.dimcrossdbprofile;

GO
