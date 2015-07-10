var defaultBomVersion = "Angel.SR3";
var boms = {
  "1.0.0.BUILD-SNAPSHOT": "Brixton.BUILD-SNAPSHOT"
};
function findBom(version) {
    if (boms[version]) return boms[version];
    return defaultBomVersion;
}

