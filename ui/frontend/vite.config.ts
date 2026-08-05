import { defineConfig } from 'vite'
import { svelte } from '@sveltejs/vite-plugin-svelte'

export default defineConfig({
  plugins: [svelte()],
  // Emit asset URLs RELATIVE to index.html, not root-absolute.
  //
  // The dashboard's documented mount is a sub-path -- the Handler godoc, the
  // README and six pages under docs/ all show
  //   mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage)))
  // -- and the mount point is not knowable at build time. With root-absolute
  // URLs the browser asked for /assets/index-*.js, which is outside the "/jobs/"
  // pattern, so the surrounding mux 404'd it and the SPA never booted: a blank
  // page. Relative URLs resolve against the document, so one build works at any
  // mount, including the root one.
  //
  // Safe because routing is HASH-based (see App.svelte): every SPA route lives in
  // the fragment, so the document's own path is always the mount root and
  // "./assets/..." cannot resolve one directory too deep.
  //
  // build:demo overrides this with an explicit --base for GitHub Pages.
  base: './',
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/jobs.v1.JobsService': 'http://localhost:8080',
    },
  },
})
