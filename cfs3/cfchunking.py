import numpy as np
import math

def get_optimal_chunkshape(f, volume, word_size=4,logger=None):
    """ 
    Given a CF field, f get an optimal chunk shape using knowledge about the various dimensions.
    Our working assumption is that we want to have, for
     - hourly data, chunk shapes which are multiples of 12 in the time dimension
     - sub-daily data, chunk shapes which divide into a small multiple of 24
     - daily data, chunk shapes which are a multiple of 10
     - monthly data, chunk shapes which are a multiple of 12
    """

    default = get_chunkshape(np.array(f.data.shape), volume, word_size, logger)
    t_axis = f.coordinate('time', None)
    if t_axis is None:
        raise ValueError('Cannot identify a time axis, optimal chunk shape not possible')
    t_data = t_axis.get_data()
    interval ='u'
    if len(t_data)> 1:
        t_delta = (t_data[1]-t_data[0]).array[0]
        t_units = t_axis.units
        if t_units.startswith('days'):
            if t_delta < 1:
                t_delta = round(t_delta*24)
                if t_delta == 1:
                    interval = 'h'
                else:
                    interval = int(24/t_delta)
            elif t_delta == 1:
                interval = 'd'
            else:
                interval = 'm'

    try:
        T = f.domain_axis('T', key=True)
        index = f.get_data_axes().index(T)
        guess = default[index]
        match interval:
            case 'h':
                if guess < 3:
                     default[index] = 2
                elif guess < 6:
                     default[index] = 4
                elif guess < 12:
                     default[index] = 6
                elif guess < 19:
                     default[index] = 12
                else:
                    default[index] = round(guess/24)*24
            case 'd':
                default[index] = round(guess/10)*10
            case 'm':
                default[index] = round(guess/12)*12       
            case 'u':
                pass
            case _:
                default[index] = int(guess/interval)*interval
        if default[index] == 0: 
            default[index] = guess # well that clearly won't work so revert
        if guess != default[index] and logger:
            logger.info(f'Time chunk changed from {guess} to {default[index]}')
    except ValueError:
        pass
    return default

def get_chunkshape(shape, volume, word_size=4, logger=None, scale_tol=0.8):
    """
    Given a shape tuple, and byte size for the elements, calculate a suitable chunk shape
    for a given volume (in bytes). (We use word instead of dtype in case the user
    changes the data type within the writing operation.)
    """

    def constrained_largest_divisor(number, constraint):
        """ 
        Find the largest divisor of number which is less than the constraint
        """
        for i in range(int(constraint), 1, -1):
            if number % i == 0:
                return i
        return 1

    def revise(dimension, guess):
        """ 
        We need the largest integer (down) less than guess 
        which is a factor of dimension, and we need
        to know how much smaller than guess it is,
        so that other dimensions can be scaled out.
        """
        old_guess = guess
        # there must be a more elegant way of doing this
        guess = constrained_largest_divisor(dimension,old_guess)
        scale_factor = old_guess/guess
        return scale_factor, guess

    v = volume/word_size 
    size = np.prod(shape)
    
    n_chunks = int(size/v)
    root = v**(1/shape.size)

    # first get a scaled set of initial guess divisors
    initial_root=np.full(shape.size, root)
    ratios = [x/min(shape) for x in shape]
    other_root = 1.0/(shape.size-1)
    indices = list(range(shape.size))
    for i in indices:
        factor = ratios[i]**other_root
        initial_root[i] = initial_root[i]*ratios[i]
        for j in indices:
            if j==i:
                continue
            initial_root[j] = initial_root[j]/factor
    
    weights_scaling = np.ones(shape.size)

    results = []
    remaining = 1
    for i in indices:
        # can't use zip because we are modifying weights in the loop
        d = shape[i]
        initial_guess = math.ceil(initial_root[i]*weights_scaling[i])
        if d%initial_guess == 0:
            results.append(initial_guess)
        else:
            scale_factor, next_guess = revise(d, initial_guess) 
            results.append(next_guess)
            if remaining < shape.size:
                scale_factor = scale_factor ** (1/(shape.size-remaining))
                weights_scaling[remaining:] = np.full(shape.size-remaining,scale_factor)
        remaining += 1 
        # fix up the last indice as we could have drifted quite small
        if i == indices[-1]:
            size_so_far = np.prod(np.array(results))
            scale_miss = size_so_far/v
            if scale_miss < scale_tol:
                constraint = results[-1]/(scale_miss)
                scaled_up=constrained_largest_divisor(shape[-1],constraint)
                results[-1]=scaled_up

    if logger:
        actual_n_chunks = int(np.prod(np.divide(shape,np.array(results))))
        cvolume =  int(np.prod(np.array(results)) * 4)
        logger.info(f'Chunk size {results} - wanted {int(n_chunks)}/{int(volume)}B will get {actual_n_chunks}/{cvolume}B')
    return results