CREATE TABLE [silver].[DimChannel]
(
    [ChannelKey] INT NOT NULL,
    [ChannelCode] NVARCHAR(20) NOT NULL,
    [ChannelName] NVARCHAR(100) NOT NULL,
    CONSTRAINT [PK_silver_DimChannel] PRIMARY KEY ([ChannelKey])
)
GO
