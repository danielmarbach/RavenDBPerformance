USE [PerformanceTestSimpleData]
GO

/****** Object:  Table [dbo].[Users]    Script Date: 3/14/2013 11:02:17 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Datas](
	[Id] [int] IDENTITY(1,1) NOT NULL,
	[Counter] [bigint] NOT NULL,
 CONSTRAINT [PK_Users_1] PRIMARY KEY CLUSTERED 
(
	[Id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

USE [PerformanceTestSimpleData]
GO

/****** Object:  Table [dbo].[Stats]    Script Date: 3/14/2013 11:03:31 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[Statistics](
	[TimeInMs] [bigint] NOT NULL,
	[Description] [varchar](255) NOT NULL,
	[NumberOfDocuments] [bigint] NOT NULL,
	[DocsPerSecond] [bigint] NOT NULL,
	[At] [datetimeoffset](7) NOT NULL
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO



