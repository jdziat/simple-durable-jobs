package ui

import "embed"

// frontendFS embeds the built frontend assets.
// The frontend must be built before compiling (npm run build in frontend/).
//
//go:embed frontend/dist
var frontendFS embed.FS
