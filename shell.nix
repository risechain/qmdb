let
  pkgs =
    import
      # Unstable for now for Rust 1.86
      (fetchTarball "https://github.com/NixOS/nixpkgs/archive/6b1c028bce9c89e9824cde040d6986d428296055.tar.gz")
      { };
in
pkgs.mkShell {
  packages = with pkgs; [
    cargo
    cargo-nextest
    rustPlatform.bindgenHook # For Reth's MDBX binding
  ];
  hardeningDisable = [ "fortify" ]; # This Nix-injected flag breaks jemalloc debug build
}
