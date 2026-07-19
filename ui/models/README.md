# Model checkpoints (Gradio)

This folder is **gitignored** (except this README). Copy trained artefacts from Colab `My Drive/runs/` into `ui/models/` before running the demo:

```
ui/models/
├── tfidf_pipeline.joblib          ← runs/text_tfidf_baseline/
├── model/                         ← runs/text_distilbert_baseline/model/
│   ├── config.json, tokenizer.*
│   └── model.safetensors           ← ~255 MB
├── resnet18_state.pt              ← runs/image_resnet18_baseline/
├── late_fusion_combiner.pkl       ← runs/fusion_late_logistic/
├── early_fusion_head.pt           ← runs/fusion_early_concat/
└── attention_fusion_head.pt       ← runs/fusion_attention/
```

Fusion models need the unimodal files (`model/`, `resnet18_state.pt`) as well.

**After cloning:** copy the files above from Drive. Without `model.safetensors`, DistilBERT and all fusion models will not run (TF-IDF and ResNet still work once their files are present).
