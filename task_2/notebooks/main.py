from pipeline_a import pipeline_A_optimized
from pipeline_a import pipeline_A_UNoptimized
from pipeline_b import pipeline_B_optimized
from pipeline_b import pipeline_B_unoptimized


if __name__ == "__main__":
    pipeline_A_optimized()
    pipeline_A_UNoptimized()

    pipeline_B_optimized()
    pipeline_B_unoptimized()