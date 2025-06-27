from beebop.services.cluster_service import get_cluster_num, get_lowest_cluster


def test_get_cluster_num_with_numeric_part():
    assert get_cluster_num("cluster123") == "123"


def test_get_cluster_num_with_no_numeric_part():
    assert get_cluster_num("cluster") == "cluster"


def test_get_cluster_num_with_multiple_numeric_parts():
    assert get_cluster_num("cluster123abc456") == "123"


def test_get_cluster_num_with_empty_string():
    assert get_cluster_num("") == ""


def test_get_cluster_num_with_special_characters():
    assert get_cluster_num("cluster@#123") == "123"


def test_get_cluster_num_with_only_numbers():
    assert get_cluster_num("123") == "123"


def test_get_cluster_num_with_leading_zeros():
    assert get_cluster_num("cluster007") == "007"


def test_get_lowest_cluster_single_cluster():
    assert get_lowest_cluster("5") == 5


def test_get_lowest_cluster_multiple_clusters():
    assert get_lowest_cluster("3;1;5;2") == 1


def test_get_lowest_cluster_duplicate_clusters():
    assert get_lowest_cluster("3;1;3;2") == 1


def test_get_lowest_cluster_ordered_clusters():
    assert get_lowest_cluster("1;2;3;4") == 1


def test_get_lowest_cluster_reverse_ordered():
    assert get_lowest_cluster("4;3;2;1") == 1


def test_get_lowest_cluster_with_zeros():
    assert get_lowest_cluster("0;5;3") == 0


def test_get_lowest_cluster_negative_numbers():
    assert get_lowest_cluster("-1;2;3") == -1


def test_get_lowest_cluster_mixed_positive_negative():
    assert get_lowest_cluster("-5;10;-2;7") == -5
