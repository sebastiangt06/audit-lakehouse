# Multi-stage build for the .NET core (API now; relay and loadgen reuse it in later steps).
# Tags pinned and verified with `docker manifest inspect`.
FROM mcr.microsoft.com/dotnet/sdk:10.0.404-noble AS build
WORKDIR /src

# Restore first (cached layer as long as project files do not change).
COPY Directory.Build.props ./
COPY Makers.Core.Api/Makers.Core.Api.csproj Makers.Core.Api/
RUN dotnet restore Makers.Core.Api/Makers.Core.Api.csproj

COPY Makers.Core.Api/ Makers.Core.Api/
RUN dotnet publish Makers.Core.Api/Makers.Core.Api.csproj -c Release --no-restore -o /app

# Chiseled = distroless Ubuntu: no shell, no package manager, non-root by default.
# "-extra" adds ICU + tzdata, needed by Microsoft.Data.SqlClient (collations / culture data).
FROM mcr.microsoft.com/dotnet/aspnet:10.0.15-noble-chiseled-extra AS runtime
WORKDIR /app
COPY --from=build /app ./
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "Makers.Core.Api.dll"]
