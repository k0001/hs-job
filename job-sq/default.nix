{ mkDerivation, aeson, base, contravariant, di, job, lib, random
, resourcet, resourcet-extra, safe-exceptions, sq, stm, time
, transformers
}:
mkDerivation {
  pname = "job-sq";
  version = "0.1";
  src = ./.;
  libraryHaskellDepends = [
    aeson base contravariant job resourcet resourcet-extra
    safe-exceptions sq stm time transformers
  ];
  testHaskellDepends = [ base di job random resourcet sq time ];
  homepage = "https://github.com/k0001/hs-job";
  description = "Job queue. Sq backend.";
  license = lib.licenses.asl20;
}
