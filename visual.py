# 在后台Server对图片推理需要两步
# 1. 激活conda环境 conda activate mmdet_lww
# 2. 运行脚本 python visual.py

# 后台Server中的可视化脚本
from mmdet.apis import DetInferencer
"""
python tools/analysis_tools/analyze_results.py configs/ddq/ddq-detr-4scale_r50_8xb2-24e_xunfei_decoder_head_EfficientKAN.py results/ddq-detr-4scale_r50_8xb2-24e_xunfei_decoder_head_EfficientKAN.pkl visual_results/ddq-detr-kan --show
"""

"""
python tools/analysis_tools/analyze_results.py configs/cell_xunfei/ddq-detr-4scale_r50_8xb2-24e_xunfei_paco_test_neck_fusion_5fold.py results/ddq_pacotest_neckfusion_xunfei/ddq_pacotest_neckfusion_xunfei.pkl visual_results/ddq_pacotest_neckfusion_xunfei --show
"""
# Initialize the DetInferencer
inferencer = DetInferencer(model='/root/data1/wxx/mmdetection_lww/configs/ddq/ddq-detr-4scale_r50_8xb2-24e_xunfei_decoder_head_EfficientKAN.py', weights='/root/data1/wxx/mmdetection_lww/work_dirs/ddq-detr-4scale_r50_8xb2-24e_xunfei_decoder_head_EfficientKAN/epoch_24.pth')
import sys
import os

def main(argv):
	if len(argv) < 2:
		print("Usage: python visual.py <input_image> [out_dir] [pred_score_thr]")
		return 1

	input_image = argv[1]
	out_dir = argv[2] if len(argv) >= 3 else 'outputs'
	pred_score = float(argv[3]) if len(argv) >= 4 else 0.3

	os.makedirs(out_dir, exist_ok=True)
	inferencer(input_image, out_dir=out_dir, pred_score_thr=pred_score)
	return 0

if __name__ == '__main__':
	sys.exit(main(sys.argv))