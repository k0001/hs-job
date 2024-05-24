{ mkDerivation, base, containers, lib, mmzk-typeid, random
, resourcet, resourcet-extra, safe-exceptions, stm, time
}:
mkDerivation {
  pname = "job";
  version = "0.1";
  src = ./.;
  libraryHaskellDepends = [
    base containers mmzk-typeid resourcet resourcet-extra
    safe-exceptions stm time
  ];
  testHaskellDepends = [ base random resourcet time ];
  homepage = "https://github.com/k0001/hs-job";
  description = "Job queue";
  license = lib.licenses.asl20;
}
