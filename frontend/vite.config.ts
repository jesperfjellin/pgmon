import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    watch: {
      usePolling: true, // Required for Docker on some systems
      interval: 2000,
    },
    hmr: {
      host: 'localhost',
    },
  },
  build: {
    outDir: "dist",
    sourcemap: true
  }
});
