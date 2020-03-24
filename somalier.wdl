task somalier_extract {
    # requires $sample to match name defined in @RG -- https://github.com/brentp/somalier/blob/master/src/somalier.nim#L25
    String sample_id
    File alignments
    File alignments_index
    File fasta
    File fasta_index

    # known sites
    File sites_vcf

    Int disk_size = 100
    Int memory = 8
    String image = "brentp/somalier:v0.2.9"

    command {
        somalier extract --out-dir `pwd`/ --fasta ${fasta} --sites ${sites_vcf} ${alignments}
    }
    runtime {
        memory: memory + "GB"
        cpu: 1
        disks: "local-disk " + disk_size + " HDD"
        preemptible: 2
        docker: image
    }
    output {
        File counts = "${sample_id}.somalier"
    }
    meta {
        author: "Joe Brown"
        email: "brwnjm@gmail.com"
        description: "Run @brentp's somalier extract across alignments (bam/cram)"
    }
}


task somalier_relate {
    Array[File] somalier_counts
    File ped

    Int disk_size = 100
    Int memory = 8
    String image = "brentp/somalier:v0.2.9"

    command {
        somalier relate --ped ${ped} ${sep=" " somalier_counts}
    }
    runtime {
        memory: memory + "GB"
        cpu: 1
        disks: "local-disk " + disk_size + " HDD"
        preemptible: 2
        docker: image
    }
    output {
        File somalier_pairs = "somalier.pairs.tsv"
        File somalier_samples = "somalier.samples.tsv"
        File somalier_html = "somalier.html"
    }
    meta {
        author: "Joe Brown"
        email: "brwnjm@gmail.com"
        description: "Run @brentp's somalier relate across somalier extracted count files"
    }
}


task tar_czf {
    Array[File] files
    String filename = "archive"

    Int disk_size = 100
    Int memory = 8
    String image = "brentp/somalier:v0.2.9"

    command {
        tar -czvf `pwd`/${filename}.tar.gz --files-from=${write_lines(files)}
    }
    runtime {
        memory: memory + "GB"
        cpu: 1
        disks: "local-disk " + disk_size + " HDD"
        preemptible: 2
        docker: image
    }
    output {
        File archive = "${filename}.tar.gz"
    }
    meta {
        author: "Joe Brown"
        email: "brwnjm@gmail.com"
        description: "Compress and array of files into a single tar.gz archive"
    }
}


workflow somalier {
    # three column TSV: sample_id, cram/bam, cram/bam_index
    File manifest
    Array[Array[String]] sample_data = read_tsv(manifest)
    File fasta
    File fasta_index
    File sites_vcf
    # File ped

    Int disk_size = 100
    Int memory = 8
    String image = "brentp/somalier:v0.2.9"

    scatter (sample in sample_data) {
        call somalier_extract {
            input:
                sample_id = sample[0],
                alignments = sample[1],
                alignments_index = sample[2],
                fasta = fasta,
                fasta_index = fasta_index,
                sites_vcf = sites_vcf,
                disk_size = disk_size,
                memory = memory,
                image = image
        }
    }
    # call somalier_relate {
    #     input:
    #         somalier_counts = somalier_extract.counts,
    #         ped = ped,
    #         disk_size = disk_size,
    #         memory = memory,
    #         image = image
    # }
    call tar_czf {
        input:
            files = somalier_extract.counts,
            filename = "somalier_counts",
            disk_size = disk_size,
            memory = memory,
            image = image
    }
    meta {
        author: "Joe Brown"
        email: "brwnjm@gmail.com"
        description: "Run @brentp's somalier on alignments for fast sample-swap and relatedness checks"
    }
}
