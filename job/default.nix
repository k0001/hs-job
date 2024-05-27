{ mkDerivation, async, base, containers, hashable, lib, mmzk-typeid
, random, resourcet, resourcet-extra, safe-exceptions, stm, time
, transformers
}:
mkDerivation {
  pname = "job";
  version = "0.1.1";
  src = ./.;
  libraryHaskellDepends = [
    async base containers hashable mmzk-typeid resourcet
    resourcet-extra safe-exceptions stm time transformers
  ];
  testHaskellDepends = [ base random resourcet time ];
  homepage = "https://github.com/k0001/hs-job";
  description = "Job queue";
  license = lib.licenses.asl20;
}
