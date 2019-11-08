package blackbox.iface;

public interface ForkedProcessMaker<T extends ForkedProcess> {
	T make(int id, String binaryBase, String configBase);
}