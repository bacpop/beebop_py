# Glossary

### Sketch
A sparse representation of some features from a genetic sequence needed for the PopPUNK algorithm.

### Cluster
The result from PopPUNKs assign function. Clusters are groups of bacterial isolates that are genetically closely related to each other. Usually the cluster is denoted by a number, but it is possible that adding new samples leads to merged clusters, which are then denoted as clusternumber1_clusternumber2.

### Component
When generating a network, the many different clusters will stay separated from each other, i.e. not being connected by edges between clusters. Each of these separate parts of the network make up one component. The numbering of the components is arbitrary and not consistent with cluster numbers.

### file-hash
For all uploaded fasta files a hash is generated from the file content. Sketches resulting from these fasta files are stored by their filehash rather than filename, to avoid saving the same file multiple times, when only the filename has changed.

### project-hash
To define a project by it's unique combination of files and their filenames, a project hash is generated by hashing a string that combines all filehashes and filenames.

### Microreact
A web tool that allows to display data for genomic epidemiology. Input files will be either a .microreact file, or a combination of a .csv, a .dot and a .nwk file. There is also an API to generate a URL that opens a project with the data already being uploaded

### Graphml
Xml file format to define networks.
