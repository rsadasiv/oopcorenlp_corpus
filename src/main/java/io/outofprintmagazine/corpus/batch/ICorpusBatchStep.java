package io.outofprintmagazine.corpus.batch;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.outofprintmagazine.corpus.batch.model.CorpusBatchStepModel;
import io.outofprintmagazine.corpus.storage.IScratchStorage;
import io.outofprintmagazine.util.IParameterStore;

public interface ICorpusBatchStep {

	void setParameterStore(IParameterStore parameterStore);

	void setStorage(IScratchStorage storage);

	CorpusBatchStepModel getData();

	void setData(CorpusBatchStepModel data);

	ObjectNode getDefaultProperties();

	ArrayNode run(ArrayNode input);

	ArrayNode runOne(ObjectNode input) throws Exception;

}