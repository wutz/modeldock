/*
Copyright 2026 Taizeng Wu.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package modelmeta

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

// InspectOptions controls pre-flight directory scanning and security policies.
type InspectOptions struct {
	AllowUnsafeFormats    bool
	IncludeTrainingStates bool
	IncludePatterns       []string
	ExcludePatterns       []string
}

// WeightFile describes a single model weight file discovered during pre-flight inspection.
type WeightFile struct {
	RelPath               string
	AbsPath               string
	Size                  int64
	SafetensorsHeaderSize int64
}

// ConfigFile describes a small config/tokenizer/metadata file packed into tar+zstd.
type ConfigFile struct {
	RelPath string
	AbsPath string
	Size    int64
}

// InspectionResult summarizes extracted ModelDock metadata and classified files.
type InspectionResult struct {
	ConfigFiles         []ConfigFile
	WeightFiles         []WeightFile
	TotalSizeBytes      int64
	WeightSizeBytes     int64
	UnsafeSerialization bool
	ServingProfile      *modeldockv1alpha1.ServingProfile
	LoRASpec            *modeldockv1alpha1.LoRASpec
}

const (
	defaultRecommendedEngine = "vllm"
	precisionFP8             = "FP8"
	precisionINT8            = "INT8"
)

var defaultTrainingStateExcludes = []string{
	"optimizer.pt",
	"optimizer*.pt",
	"optimizer*.bin",
	"scheduler.pt",
	"rng_state*.pth",
	"rng_state*.pt",
	"zero_pp_rank_*",
	"zero_stage*",
	"global_step*",
	"wandb/**",
	".git/**",
}

// InspectDir performs security validation, file classification, and metadata extraction on a local model directory.
func InspectDir(
	root string,
	artifactType modeldockv1alpha1.ArtifactType,
	opts InspectOptions,
) (*InspectionResult, error) {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve model directory %q: %w", root, err)
	}
	info, err := os.Stat(realRoot)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("%q is not a directory", root)
	}

	res := &InspectionResult{
		ServingProfile: &modeldockv1alpha1.ServingProfile{
			RecommendedEngine: defaultRecommendedEngine,
		},
	}

	err = filepath.WalkDir(realRoot, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == realRoot {
			return nil
		}
		return inspectWalkEntry(realRoot, path, d, opts, res)
	})
	if err != nil {
		return nil, err
	}

	if len(res.WeightFiles) == 0 {
		return nil, errors.New("no valid weight files found in model directory")
	}

	// Verify model.safetensors.index.json completeness if present.
	if err := verifySafetensorsIndex(realRoot, res.WeightFiles); err != nil {
		return nil, err
	}

	// Parse metadata files to populate ServingProfile and LoRASpec.
	if err := PopulateMetadataFromDir(realRoot, artifactType, res); err != nil {
		return nil, err
	}
	return res, nil
}

func inspectWalkEntry(
	realRoot, path string,
	d fs.DirEntry,
	opts InspectOptions,
	res *InspectionResult,
) error {
	// Symlink escape defense.
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("cannot resolve path %q: %w", path, err)
	}
	relCheck, err := filepath.Rel(realRoot, resolved)
	if err != nil || strings.HasPrefix(relCheck, "..") || filepath.IsAbs(relCheck) {
		return fmt.Errorf("security violation: path %q escapes model root via symlink", path)
	}

	rel, err := filepath.Rel(realRoot, path)
	if err != nil {
		return err
	}
	relSlash := filepath.ToSlash(rel)

	if shouldSkipPath(relSlash, opts) {
		if d.IsDir() {
			return filepath.SkipDir
		}
		return nil
	}
	if d.IsDir() {
		return nil
	}
	if len(opts.IncludePatterns) > 0 && !matchesAnyPattern(relSlash, opts.IncludePatterns) {
		return nil
	}

	fi, err := d.Info()
	if err != nil {
		return err
	}
	size := fi.Size()
	if !isWeightFile(relSlash) {
		res.ConfigFiles = append(res.ConfigFiles, ConfigFile{
			RelPath: relSlash,
			AbsPath: resolved,
			Size:    size,
		})
		res.TotalSizeBytes += size
		return nil
	}
	return recordWeightFile(relSlash, resolved, size, opts, res)
}

func shouldSkipPath(relSlash string, opts InspectOptions) bool {
	if !opts.IncludeTrainingStates && matchesAnyPattern(relSlash, defaultTrainingStateExcludes) {
		return true
	}
	return matchesAnyPattern(relSlash, opts.ExcludePatterns)
}

func recordWeightFile(
	relSlash, resolved string,
	size int64,
	opts InspectOptions,
	res *InspectionResult,
) error {
	if size == 0 {
		return fmt.Errorf("weight file %q is empty", relSlash)
	}
	if isUnsafePickleWeight(relSlash) {
		if !opts.AllowUnsafeFormats {
			return fmt.Errorf(
				"unsafe pickle weight format %q blocked (set allowUnsafeFormats=true to override)",
				relSlash,
			)
		}
		res.UnsafeSerialization = true
	}
	var hdrSize int64
	if strings.HasSuffix(strings.ToLower(relSlash), ".safetensors") {
		hs, _, _, err := InspectSafetensorsFile(resolved)
		if err != nil {
			return fmt.Errorf("invalid safetensors file %q: %w", relSlash, err)
		}
		hdrSize = hs
	}
	res.WeightFiles = append(res.WeightFiles, WeightFile{
		RelPath:               relSlash,
		AbsPath:               resolved,
		Size:                  size,
		SafetensorsHeaderSize: hdrSize,
	})
	res.WeightSizeBytes += size
	res.TotalSizeBytes += size
	return nil
}

// PopulateMetadataFromDir parses config.json, tokenizer_config.json, adapter_config.json, and weight stats.
func PopulateMetadataFromDir(dir string, artifactType modeldockv1alpha1.ArtifactType, res *InspectionResult) error {
	if res.ServingProfile == nil {
		res.ServingProfile = &modeldockv1alpha1.ServingProfile{RecommendedEngine: defaultRecommendedEngine}
	}

	// Determine serialization format.
	res.ServingProfile.SerializationFormat = detectSerializationFormat(res.WeightFiles)

	// Read config.json if present.
	if raw, err := os.ReadFile(filepath.Join(dir, "config.json")); err == nil {
		ParseModelConfigJSON(raw, res.ServingProfile)
	}

	// Read tokenizer_config.json or chat_template.jinja.
	if raw, err := os.ReadFile(filepath.Join(dir, "tokenizer_config.json")); err == nil {
		var tok map[string]any
		if json.Unmarshal(raw, &tok) == nil {
			if ct, ok := tok["chat_template"]; ok && ct != nil && ct != "" {
				res.ServingProfile.HasChatTemplate = true
			}
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "chat_template.jinja")); err == nil {
		res.ServingProfile.HasChatTemplate = true
	}

	// Check LoRA adapter_config.json.
	if raw, err := os.ReadFile(filepath.Join(dir, "adapter_config.json")); err == nil {
		lora, baseArch := ParseAdapterConfigJSON(raw)
		res.LoRASpec = lora
		if res.ServingProfile.Architecture == "" && baseArch != "" {
			res.ServingProfile.Architecture = baseArch
		}
	} else if artifactType == modeldockv1alpha1.ArtifactTypeLoRAAdapter {
		return errors.New("adapter_config.json is required for LoRAAdapter artifacts")
	}

	// Infer parameter size / precision / VRAM estimates from weight files if missing.
	FinalizeServingProfile(artifactType, res.WeightFiles, res.WeightSizeBytes, res.ServingProfile)
	return nil
}

// ParseModelConfigJSON extracts architecture, context length, precision, and trust_remote_code hints from config.json.
func ParseModelConfigJSON(raw []byte, sp *modeldockv1alpha1.ServingProfile) {
	var cfg map[string]any
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return
	}
	if archs, ok := cfg["architectures"].([]any); ok && len(archs) > 0 {
		if s, ok := archs[0].(string); ok {
			sp.Architecture = s
		}
	}
	if sp.Architecture == "" {
		if mt, ok := cfg["model_type"].(string); ok {
			sp.Architecture = mt
		}
	}
	if mpe, ok := cfg["max_position_embeddings"].(float64); ok && mpe > 0 {
		sp.MaxPositionEmbeddings = int32(mpe)
	}
	if _, hasAutoMap := cfg["auto_map"]; hasAutoMap {
		sp.RequiresTrustRemoteCode = true
	}
	if dtype, ok := cfg["torch_dtype"].(string); ok {
		sp.Precision = normalizePrecision(dtype)
	}
	if qcfg, ok := cfg["quantization_config"].(map[string]any); ok {
		if qm, ok := qcfg["quant_method"].(string); ok {
			bits, _ := qcfg["bits"].(float64)
			switch strings.ToLower(qm) {
			case "awq":
				sp.Precision = "INT4-AWQ"
			case "gptq":
				sp.Precision = "INT4-GPTQ"
			case "fp8":
				sp.Precision = precisionFP8
			default:
				if bits == 8 {
					sp.Precision = precisionINT8
				}
			}
		}
	}
	// Estimate parameter count from Transformer config if available.
	if sp.ParameterSize == "" {
		if est := estimateParamsFromConfig(cfg); est > 0 {
			sp.ParameterSize = FormatParamCount(est)
		}
	}
}

// ParseAdapterConfigJSON parses PEFT adapter_config.json into LoRASpec.
func ParseAdapterConfigJSON(raw []byte) (*modeldockv1alpha1.LoRASpec, string) {
	var cfg struct {
		R                   int32    `json:"r"`
		LoraAlpha           int32    `json:"lora_alpha"`
		TargetModules       []string `json:"target_modules"`
		BaseModelNameOrPath string   `json:"base_model_name_or_path"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.R <= 0 {
		return nil, ""
	}
	return &modeldockv1alpha1.LoRASpec{
		Rank:          cfg.R,
		Alpha:         cfg.LoraAlpha,
		TargetModules: cfg.TargetModules,
	}, cfg.BaseModelNameOrPath
}

// InspectSafetensorsFile validates a .safetensors header and returns (headerSize, totalParams, primaryDtype, err).
func InspectSafetensorsFile(path string) (int64, int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, "", err
	}
	defer func() { _ = f.Close() }()

	var lenBuf [8]byte
	if _, err := io.ReadFull(f, lenBuf[:]); err != nil {
		return 0, 0, "", fmt.Errorf("read safetensors header length: %w", err)
	}
	headerLen := binary.LittleEndian.Uint64(lenBuf[:])
	if headerLen == 0 || headerLen > 64*1024*1024 {
		return 0, 0, "", fmt.Errorf("implausible safetensors header length %d", headerLen)
	}
	hdrBytes := make([]byte, headerLen)
	if _, err := io.ReadFull(f, hdrBytes); err != nil {
		return 0, 0, "", fmt.Errorf("read safetensors header JSON: %w", err)
	}
	var hdr map[string]json.RawMessage
	if err := json.Unmarshal(hdrBytes, &hdr); err != nil {
		return 0, 0, "", fmt.Errorf("parse safetensors header JSON: %w", err)
	}

	var totalParams int64
	dtypeCounts := map[string]int64{}
	for name, rawMeta := range hdr {
		if name == "__metadata__" {
			continue
		}
		var tm struct {
			DType string  `json:"dtype"`
			Shape []int64 `json:"shape"`
		}
		if err := json.Unmarshal(rawMeta, &tm); err != nil {
			continue
		}
		elems := int64(1)
		for _, dim := range tm.Shape {
			elems *= dim
		}
		if len(tm.Shape) == 0 {
			elems = 0
		}
		totalParams += elems
		if tm.DType != "" {
			dtypeCounts[tm.DType] += elems
		}
	}

	primaryDtype := ""
	var maxCount int64
	for dt, c := range dtypeCounts {
		if c > maxCount {
			maxCount = c
			primaryDtype = dt
		}
	}
	return int64(headerLen), totalParams, primaryDtype, nil
}

// FinalizeServingProfile fills precision, parameter size, VRAM estimate, and TP recommendation.
func FinalizeServingProfile(
	artifactType modeldockv1alpha1.ArtifactType,
	weights []WeightFile,
	weightBytes int64,
	sp *modeldockv1alpha1.ServingProfile,
) {
	if sp.RecommendedEngine == "" {
		sp.RecommendedEngine = defaultRecommendedEngine
	}
	if sp.SerializationFormat == "" {
		sp.SerializationFormat = detectSerializationFormat(weights)
	}
	var totalParams int64
	var primaryDtype string
	for _, w := range weights {
		if w.AbsPath != "" && strings.HasSuffix(strings.ToLower(w.RelPath), ".safetensors") {
			_, p, dt, err := InspectSafetensorsFile(w.AbsPath)
			if err == nil {
				totalParams += p
				if primaryDtype == "" && dt != "" {
					primaryDtype = dt
				}
			}
		}
	}
	if sp.Precision == "" && primaryDtype != "" {
		sp.Precision = normalizePrecision(primaryDtype)
	}
	if sp.Precision == "" {
		sp.Precision = "Unknown"
	}
	if sp.ParameterSize == "" && totalParams > 0 {
		sp.ParameterSize = FormatParamCount(totalParams)
	}

	if artifactType == modeldockv1alpha1.ArtifactTypeLoRAAdapter {
		vram := max(int32(math.Ceil(float64(weightBytes)/(1024*1024*1024)))+1, 1)
		if sp.MinVRAMGiB == 0 {
			sp.MinVRAMGiB = vram
		}
		return
	}

	if sp.MinVRAMGiB == 0 {
		weightGiB := float64(weightBytes) / (1024 * 1024 * 1024)
		// Add ~15% KV cache & activation headroom, minimum 1 GiB.
		est := max(int32(math.Ceil(weightGiB*1.15)), 1)
		sp.MinVRAMGiB = est
	}
	if sp.RecommendedTensorParallel == 0 {
		switch {
		case sp.MinVRAMGiB <= 70:
			sp.RecommendedTensorParallel = 1
		case sp.MinVRAMGiB <= 140:
			sp.RecommendedTensorParallel = 2
		case sp.MinVRAMGiB <= 280:
			sp.RecommendedTensorParallel = 4
		default:
			sp.RecommendedTensorParallel = 8
		}
	}
}

func verifySafetensorsIndex(root string, weights []WeightFile) error {
	indexPath := filepath.Join(root, "model.safetensors.index.json")
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var idx struct {
		WeightMap map[string]string `json:"weight_map"`
	}
	if err := json.Unmarshal(raw, &idx); err != nil {
		return fmt.Errorf("invalid model.safetensors.index.json: %w", err)
	}
	present := make(map[string]struct{}, len(weights))
	for _, w := range weights {
		present[w.RelPath] = struct{}{}
	}
	for tensorName, shard := range idx.WeightMap {
		cleanShard := filepath.ToSlash(filepath.Clean(shard))
		if _, ok := present[cleanShard]; !ok {
			return fmt.Errorf("model.safetensors.index.json references missing shard %q (for tensor %s)", cleanShard, tensorName)
		}
	}
	return nil
}

func detectSerializationFormat(weights []WeightFile) string {
	for _, w := range weights {
		low := strings.ToLower(w.RelPath)
		switch {
		case strings.HasSuffix(low, ".safetensors"):
			return "safetensors"
		case strings.HasSuffix(low, ".gguf"):
			return "gguf"
		case strings.HasSuffix(low, ".bin"), strings.HasSuffix(low, ".pt"), strings.HasSuffix(low, ".pth"):
			return "pytorch-bin"
		}
	}
	return "unknown"
}

func isWeightFile(rel string) bool {
	low := strings.ToLower(rel)
	for _, ext := range []string{".safetensors", ".gguf", ".bin", ".pt", ".pth", ".ckpt", ".pkl"} {
		if strings.HasSuffix(low, ext) {
			return true
		}
	}
	return false
}

func isUnsafePickleWeight(rel string) bool {
	low := strings.ToLower(rel)
	for _, ext := range []string{".bin", ".pt", ".pth", ".ckpt", ".pkl"} {
		if strings.HasSuffix(low, ext) {
			return true
		}
	}
	return false
}

func normalizePrecision(s string) string {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "BF16", "BFLOAT16":
		return "BF16"
	case "F16", "FP16", "FLOAT16":
		return "FP16"
	case "F32", "FP32", "FLOAT32":
		return "FP32"
	case "F8_E4M3", "F8_E5M2", precisionFP8, "FLOAT8_E4M3FN":
		return precisionFP8
	case "I8", precisionINT8:
		return precisionINT8
	default:
		return "Unknown"
	}
}

func estimateParamsFromConfig(cfg map[string]any) int64 {
	h, _ := cfg["hidden_size"].(float64)
	l, _ := cfg["num_hidden_layers"].(float64)
	v, _ := cfg["vocab_size"].(float64)
	inter, _ := cfg["intermediate_size"].(float64)
	if h <= 0 || l <= 0 {
		return 0
	}
	if inter <= 0 {
		inter = 4 * h
	}
	// Rough Transformer parameter count: embed + L * (4*h^2 + 3*h*inter)
	total := v*h + l*(4*h*h+3*h*inter)
	return int64(total)
}

// FormatParamCount renders an integer parameter count like "7B", "32B", or "150M".
func FormatParamCount(n int64) string {
	switch {
	case n >= 1_000_000_000:
		val := float64(n) / 1_000_000_000
		if math.Abs(val-math.Round(val)) < 0.1 {
			return fmt.Sprintf("%.0fB", math.Round(val))
		}
		return fmt.Sprintf("%.1fB", val)
	case n >= 1_000_000:
		return fmt.Sprintf("%.0fM", math.Round(float64(n)/1_000_000))
	case n >= 1_000:
		return fmt.Sprintf("%.0fK", math.Round(float64(n)/1_000))
	default:
		return fmt.Sprintf("%d", n)
	}
}

func matchesAnyPattern(relSlash string, patterns []string) bool {
	base := filepath.Base(relSlash)
	for _, pat := range patterns {
		pat = filepath.ToSlash(strings.TrimSpace(pat))
		if pat == "" {
			continue
		}
		if prefix, ok := strings.CutSuffix(pat, "/**"); ok {
			if relSlash == prefix || strings.HasPrefix(relSlash, prefix+"/") {
				return true
			}
		}
		if ok, _ := filepath.Match(pat, relSlash); ok {
			return true
		}
		if ok, _ := filepath.Match(pat, base); ok {
			return true
		}
	}
	return false
}
