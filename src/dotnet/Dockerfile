# Multi-stage build for the .NET core. Two final targets:
#   api     -> core-api and outbox-relay (same image, Role=Api|Relay)
#   loadgen -> load generator (compose profile "tools")
# BuildKit only builds the stages a target needs: `target: api` never restores or compiles the
# load generator, and the test project is never copied into any image.
# Tags pinned and verified with `docker manifest inspect`.
FROM mcr.microsoft.com/dotnet/sdk:10.0.404-noble AS build
WORKDIR /src

# Restore first (cached layer as long as project files do not change).
COPY Directory.Build.props ./
COPY Makers.Core.Api/Makers.Core.Api.csproj Makers.Core.Api/
RUN dotnet restore Makers.Core.Api/Makers.Core.Api.csproj

COPY Makers.Core.Api/ Makers.Core.Api/
RUN dotnet publish Makers.Core.Api/Makers.Core.Api.csproj -c Release --no-restore -o /app

FROM mcr.microsoft.com/dotnet/sdk:10.0.404-noble AS build-loadgen
WORKDIR /src
COPY Directory.Build.props ./
COPY Makers.Core.LoadGen/Makers.Core.LoadGen.csproj Makers.Core.LoadGen/
RUN dotnet restore Makers.Core.LoadGen/Makers.Core.LoadGen.csproj
COPY Makers.Core.LoadGen/ Makers.Core.LoadGen/
RUN dotnet publish Makers.Core.LoadGen/Makers.Core.LoadGen.csproj -c Release --no-restore -o /app

# Chiseled = distroless Ubuntu: no shell, no package manager, non-root by default.
# "-extra" adds ICU + tzdata, needed by Microsoft.Data.SqlClient (collations / culture data).
FROM mcr.microsoft.com/dotnet/aspnet:10.0.15-noble-chiseled-extra AS api
WORKDIR /app
COPY --from=build /app ./
ENV ASPNETCORE_HTTP_PORTS=8080
EXPOSE 8080
USER $APP_UID
ENTRYPOINT ["dotnet", "Makers.Core.Api.dll"]

# Same base as the API on purpose: its layers are already pulled, so the load generator only adds
# its own DLL (a separate runtime image would be one more download for a dev-only tool), and ICU
# is present because InvariantGlobalization=false (Directory.Build.props).
FROM mcr.microsoft.com/dotnet/aspnet:10.0.15-noble-chiseled-extra AS loadgen
WORKDIR /app
COPY --from=build-loadgen /app ./
USER $APP_UID
ENTRYPOINT ["dotnet", "Makers.Core.LoadGen.dll"]
CMD ["--count", "1000", "--parallel", "10"]
