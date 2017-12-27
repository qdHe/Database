class Instance {
    String label;
    List<String> fieldList = new ArrayList<String>();
    public Instance(String label, List<String> fieldList) {
        this.label = label;
        this.fieldList = fieldList;
    }
}
class Feature {
    String label;
    String value;
    public Feature(String label, String value) {
        this.label = label;
        this.value = value;
    }
    @Override
    public boolean equals(Object obj) {
        Feature feature = (Feature) obj;
        if (this.label.equals(feature.label) && this.value.equals(feature.value))
            return true;
        return false;
    }
    @Override public String toString() {
        return "[" + label + ", " + value + "]";
    }
}

List<Instance> instanceList = new ArrayList<Instance>();
List<Feature> featureList = new ArrayList<Feature>();
List<Integer> featureCountList = new ArrayList<Integer>();
List<String> labels = new ArrayList<String>();
double[] weight;

void train(int maxIt) {
    int size = featureList.size();
    weight = new double[size];
    double[] empiricalE = new double[size];
    double[] modelE = new double[size];

    for (int i = 0; i < size; ++i) {
        empiricalE[i] = (double) featureCountList.get(i) / instanceList.size();
    }

    double[] lastWeight = new double[weight.length];
    for (int i = 0; i < maxIt; ++i) {
        computeModeE(modelE);
        for (int w = 0; w < weight.length; w++) {
            lastWeight[w] = weight[w];
            weight[w] += 1.0 / C * Math.log(empiricalE[w] / modelE[w]);
        }
        if (checkConvergence(lastWeight, weight)) break;
    }
}

Pair<String, Double>[] predict(List<String> fieldList) {
    double[] prob = calProb(fieldList);
    Pair<String, Double>[] pairResult = new Pair[prob.length];
    for (int i = 0; i < prob.length; ++i) {
        pairResult[i] = new Pair<String, Double>(labels.get(i), prob[i]);
    }
    return pairResult;
}

boolean checkConvergence(double[] w1, double[] w2) {
    for (int i = 0; i < w1.length; ++i) {
        if (Math.abs(w1[i] - w2[i]) >= 0.01) return false;
    }
    return true;
}

void computeModeE(double[] modelE) {
    Arrays.fill(modelE, 0.0f);
    for (int i = 0; i < instanceList.size(); ++i) {
        List<String> fieldList = instanceList.get(i).fieldList;
        double[] pro = calProb(fieldList);
        for (int j = 0; j < fieldList.size(); j++) {
            for (int k = 0; k < labels.size(); k++) {
                Feature feature = new Feature(labels.get(k), fieldList.get(j));
                int index = featureList.indexOf(feature);
                if (index!=-1) modelE[index] += pro[k] * (1.0/instanceList.size());
            }
        }
    }
}

double[] calProb(List<String> fieldList) {
    double[] p = new double[labels.size()];
    double sum = 0; for (int i = 0; i < labels.size(); ++i) {
        double weightSum = 0;
        for (String field : fieldList) {
            Feature feature = new Feature(labels.get(i), field);
            int index = featureList.indexOf(feature);
            if (index != -1) weightSum += weight[index];
        }
        p[i] = Math.exp(weightSum); sum += p[i];
    }
    for (int i = 0; i < p.length; ++i) { p[i] /= sum; }
    return p;
}

