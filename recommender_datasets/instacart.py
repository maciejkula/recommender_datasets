from recommender_datasets import _common

URL = 'https://s3.amazonaws.com/instacart-datasets/instacart_online_grocery_shopping_2017_05_01.tar.gz?X-Amz-Expires=21600&X-Amz-Date=20170913T135517Z&X-Amz-Security-Token=FQoDYXdzEBYaDIraRhD5HZl6iYV1ISKtA4E6k%2BUhVhsThDBCCg%2BPymj50t4GZczh6GMWbT/bNaurn4EXCIK5EBA4co50nK%2BfudCsrVdzF/y2fSzGcV4/OQZDecGag6gdiBJ%2BVFWwiGrez14pHCzWrcg3dpzxx6eVdR16lQSyxE0q3XPPsQUsPWMGXS6R8V7OxOR0TIFPeRDvezRVIrEPjn5mrtbK%2BeLbgYhw84towXLzYnlS6O7CbUCaqt/VNCHF7cJRIvKYftNVFYzbnkLJdRvylCTZ2jDZe7CCA0tXzuV0ZjrUsh48g8XRN2kyEulBALEQsENarlCfkejWFJAubx6I75IsdL9afsMs4DYGDIPYyxncUWkeeZ1FqgBSy%2B39LKT5ri%2BSrNf/lhe/UrDeIaL%2BkJbgKbewoNB/OK4FSZxWPTK5CpBorfzD9uG3wuF2Bi8Jqp9koS6%2B/ZroFIgcIW207hUVgb/cP3WGEO8sgqkq61xAe2mm%2B3KTJGyR8Sb5q2HgXN75pwJWsTMSdX04cYdEl1H9oVV29H3u22MmJMkvht4yLAMD%2BXLZX5CF/Z8CfTnwyZ8Pps14saYjSIrRXx%2BYL8SyACjP3uTNBQ%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=ASIAJW2OBAV55VAOGXVA/20170913/us-east-1/s3/aws4_request&X-Amz-SignedHeaders=host&X-Amz-Signature=d1731547df67dfc7f710c4b2540b3866903849e39508c6279e13d0f53d33ed0c'  # NOQA


def get_instacart_data():

    zip_path = _common.get_data(URL,
                                'instacart',
                                'instacart.tar.gz')




if __name__ == '__main__':
    get_instacart_data()
